GeoParquet for Python is a GeoPandas API designed to facilitate fast
input/output of GIS data in the open source Parquet file format.

The project is currently a proof of concept.

__Why is this project needed?__
The GIS community currently lacks a fast, efficient, open-source file format for
persisting and sharing data with. The purpose of the GeoParquet project is to
develop such a file format.

__How does it work?__
The GeoParquet file format is simple adaptation of the existing Parquet file
format: geometries are stored as well-known binary (WKB), and the coordinate
reference system is stored as a PROJ string in the file's metadata.
