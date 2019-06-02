# load dependencies
import geopandas as gpd
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely


# ==============================================================================
# definitions for to_geoparquet()
#
# function overview:
# 0. capture geometry column name
# 1. capture CRS as proj string
# 2. serialise geometry
# 3. convert to pyarrow Table
# 4. set pyarrow Table metadata with geometry column name and CRS
# 5. write to parquet file
# ==============================================================================


def to_proj_string(crs: dict) -> str:
    """Express a dictionary of proj string parameters a a single proj string."""
    params = []
    for k, v in crs.items():
        param = ''.join(['+', k, '=', v])
        params.append(param)
    proj_string = ' '.join(params)
    return proj_string


def serialise_geometry(self: gpd.GeoDataFrame, geom_col_name: str) -> pd.DataFrame:
    """
    Given a geopandas GeoDataFrame, serialise the GeoSeries as well-known binary
    and return the former geopandas GeoDataFrame as a pandas DataFrame.
    """
    # prevent side effects
    df = self.copy()
    # serialise shapely geometry as WKB
    df[geom_col_name] = df[geom_col_name].apply(shapely.wkb.dumps)
    return df


gpd.GeoDataFrame.serialise_geometry = serialise_geometry


def set_metadata(tbl: pa.Table, tbl_meta={}) -> pa.Table:
    """
    Serialise table-level metadata as JSON-encoded byte strings.

    To update the metadata, first new fields are created for all columns.
    Next a schema is created using the new fields and updated table metadata.
    Finally a new table is created by replacing the old one's schema, but
    without copying any data.
    """
    # contributed by stackoverflow user 3519145 'thomas'

    # create updated column fields with new metadata
    if tbl_meta:
        fields = [col.field for col in tbl.itercolumns()]

        # get updated table metadata
        tbl_metadata = tbl.schema.metadata
        for k, v in tbl_meta.items():
            tbl_metadata[k] = json.dumps(v).encode('utf-8')

        # create new schema with updated table metadata
        schema = pa.schema(fields, metadata=tbl_metadata)

        # with updated schema build new table (shouldn't copy data)
        tbl = pa.Table.from_arrays(list(tbl.itercolumns()), schema=schema)

    return tbl


def to_geoparquet(self: gpd.GeoDataFrame, path: str):
    """
    Given a geopandas GeoDataFrame, serialise geometry as WKB, store geometry
    column name and CRS in Apache Arrow metadata, and write to parquet file.

    Note that GIS-specific data is stored in a dict named 'gis', much in the
    same way that pandas-specific data is stored in a dict named 'pandas'.
    """
    # capture geometry column name
    geom_col_name = self.geometry.name
    # capture CRS
    try:
        crs = to_proj_string(self.crs)
    except:
        crs = self.crs
    # serialise geometry
    self = self.serialise_geometry(geom_col_name)
    # convert to pyarrow Table
    self = pa.Table.from_pandas(self)
    # set pyarrow Table metadata with geometry column name and CRS
    self = set_metadata(
        self,
        tbl_meta={
            'gis' : {
                'geom_col_name' : geom_col_name,
                'crs' : crs,
            }
        }
    )
    # write to parquet file
    pq.write_table(self, path)


gpd.GeoDataFrame.to_geoparquet = to_geoparquet


# ==============================================================================
# definitions for gpq.read_geoparquet()
#
# function overview:
# 0. load into pyarrow Table
# 1. capture geometry column name
# 2. capture CRS
# 3. convert to pandas DataFrame
# 4. deserialise geometry column
# 5. convert to geopandas GeoDataFrame
# ==============================================================================


def deserialise_metadata(tbl: pa.Table) -> dict:
    """
    Deserialise pyarrow table metadata from UTF-8 JSON-encoded strings into dict.
    """
    metadata = tbl.schema.metadata

    if not metadata:
        # None or {} are not decoded
        return metadata

    decoded = {}
    for k, v in metadata.items():
        key = k.decode('utf-8')
        val = json.loads(v.decode('utf-8'))
        decoded[key] = val
    return decoded


def deserialise_geometry(self: pd.DataFrame, geom_col_name: str) -> pd.DataFrame:
    """Given a named column, deserialise WKB strings into shapely geometries."""
    # prevent side effects
    df = self.copy()
    # deserialise WKB to shapely geometry
    df[geom_col_name] = df[geom_col_name].apply(shapely.wkb.loads)
    return df


pd.DataFrame.deserialise_geometry = deserialise_geometry


def read_geoparquet(path: str) -> gpd.GeoDataFrame:
    """
    Given the path to a parquet file, construct a geopandas GeoDataFrame by:
    loading the file as a pyarrow table, reading the geometry column name and
    CRS from the metadata, deserialising WKB into shapely geometries, and
    building a geopandas GeoDataFrame.
    """
    # load into pyarrow Table
    table = pq.read_table(path)
    # capture geometry column name
    geom_col_name = deserialise_metadata(table)['gis']['geom_col_name']
    # capture CRS
    crs = deserialise_metadata(table)['gis']['crs']
    # convert to pandas DataFrame
    df = table.to_pandas()
    # deserialise geometry column
    df = df.deserialise_geometry(geom_col_name)
    # convert to geopandas GeoDataFrame
    return gpd.GeoDataFrame(df, crs=crs, geometry=geom_col_name)
