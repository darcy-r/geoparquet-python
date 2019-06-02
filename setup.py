import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="geoparquet",
    version="0.0.1",
    author="D'Arcy Roche",
    # author_email="d.arcy@posteo.de",
    description="GeoPandas API for Parquet file format.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/darcy-r/geoparquet-python",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Topic :: Scientific/Engineering :: GIS',
    ],
)
