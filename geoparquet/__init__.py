

import json
import multiprocessing

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely

from geopandas import GeoDataFrame


def _serialise_geometry(self: GeoDataFrame,
                        geom_col_name: str
                        ) -> pd.DataFrame:
    """
    Given a geopandas GeoDataFrame, serialise the GeoSeries as well-known
    binary and return the former geopandas GeoDataFrame as a pandas DataFrame.
    """
    # prevent side effects
    df = self.copy()
    # serialise shapely geometry as WKB
    with multiprocessing.Pool() as P:
        df[geom_col_name] = P.map(shapely.wkb.dumps, df[geom_col_name])
    return df


GeoDataFrame._serialise_geometry = _serialise_geometry


def _update_metadata(table: pa.Table, new_metadata={}) -> pa.Table:
    """
    Serialise user-defined table-level metadata as JSON-encoded byte strings
    and append to existing table metadata.
    """
    # with help from stackoverflow users 3519145 'thomas' and 289784 'suvayu'

    if new_metadata:
        # set aside original metadata
        tbl_metadata = table.schema.metadata
        # update original metadata with new metadata from user
        for k, v in new_metadata.items():
            tbl_metadata[k] = json.dumps(v).encode("utf-8")
        # replace metadata in table object
        table = table.replace_schema_metadata(tbl_metadata)
    return table


def to_geoparquet(self: GeoDataFrame, path: str):
    """
    Given a geopandas GeoDataFrame, serialise geometry as WKB, store geometry
    column name and CRS in Apache Arrow metadata, and write to parquet file.

    Metadata about geometry columns is stored in a key named 'geometry_fields'
    in the same way that pandas-specific metadata is stored in a key named
    'pandas'.
    """
    # capture geometry column name
    field_name = self.geometry.name
    # capture CRS
    crs = pyproj.CRS.from_user_input(self.crs).to_wkt(version="WKT2_2018")
    crs_format = "WKT2_2018"
    # capture geometry types
    geometry_types = self.geometry.geom_type.unique().tolist()
    # serialise geometry
    self = self._serialise_geometry(field_name)
    # convert to pyarrow Table
    self = pa.Table.from_pandas(self)
    # set pyarrow Table metadata with geometry column name and CRS
    geometry_metadata = {
        "geometry_fields": [
            {
                "field_name": field_name,
                "geometry_format": "wkb",
                "geometry_types": geometry_types,
                "crs": crs,
                "crs_format": crs_format,
            }
        ]
    }
    self = _update_metadata(self, new_metadata=geometry_metadata)
    # write to parquet file
    pq.write_table(self, path)
    return


GeoDataFrame.to_geoparquet = to_geoparquet


def _deserialise_metadata(table: pa.Table) -> dict:
    """
    Deserialise pyarrow table metadata from UTF-8 JSON-encoded strings into
    dict.
    """
    # with help from stackoverflow user 3519145 'thomas'
    metadata = table.schema.metadata
    deserialised_metadata = {}
    for k, v in metadata.items():
        key = k.decode("utf-8")
        val = json.loads(v.decode("utf-8"))
        deserialised_metadata[key] = val
    return deserialised_metadata


def _deserialise_geometry(self: pd.DataFrame,
                          geom_col_name: str
                          ) -> pd.DataFrame:
    """
    Given a named column, deserialise WKB strings into shapely geometries.
    """
    # prevent side effects
    df = self.copy()
    # deserialise WKB to shapely geometry
    with multiprocessing.Pool() as P:
        df[geom_col_name] = P.map(shapely.wkb.loads, df[geom_col_name])
    return df


pd.DataFrame._deserialise_geometry = _deserialise_geometry


def read_geoparquet(path: str) -> GeoDataFrame:
    """
    Given the path to a parquet file, construct a geopandas GeoDataFrame by:
    - loading the file as a pyarrow table
    - reading the geometry column name and CRS from the metadata
    - deserialising WKB into shapely geometries
    """
    # read parquet file into pyarrow Table
    table = pq.read_table(path)
    # deserialise metadata for first geometry field
    # (geopandas only supports one geometry column)
    geometry_metadata = _deserialise_metadata(table)["geometry_fields"][0]
    # extract CRS
    crs = geometry_metadata["crs"]
    # convert pyarrow Table to pandas DataFrame
    df = table.to_pandas()
    # identify geometry column name
    geom_col_name = geometry_metadata["field_name"]
    # deserialise geometry column
    df = df._deserialise_geometry(geom_col_name)
    # convert to geopandas GeoDataFrame
    df = GeoDataFrame(df, crs=crs, geometry=geom_col_name)
    return df
