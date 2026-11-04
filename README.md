# Shapefile Data Source for Apache Spark

A library for parsing and querying [shapefile](https://en.wikipedia.org/wiki/Shapefile) data with Apache Spark, for Spark SQL and DataFrames.

### Requirements

Two build lines:

| Profile | Spark | Scala | Java | Jar |
|---|---|---|---|---|
| `spark-3.5` (default) | 3.5.9 | 2.12 | 8+ | `spark-shp-0.33-3.5-2.12.jar` |
| `spark-4.3` | 4.3.0 | 2.13 | 17+ | `spark-shp-0.33-4.3-2.13.jar` |

The Esri Geometry API (`com.esri.geometry:esri-geometry-api:2.3.1`, Maven Central) is a `provided` dependency: put it on the classpath next to the jar.

### Using with Spark shell

```shell script
$SPARK_HOME/bin/spark-shell --jars spark-shp-0.33-3.5-2.12.jar --packages com.esri.geometry:esri-geometry-api:2.3.1
```

### Features

This package allows reading shapefiles in local or distributed filesystem as Spark DataFrames. When reading files the API accepts several options:

- `path` The location of shapefile(s). Similar to Spark can accept standard Hadoop globbing expressions.
- `shape` An optional name of the shape column. Default value is `shape`.
- `columns` An optional list of comma separated attribute column names. Default value is blank indicating all attribute fields.
- `format` An optional parameter to define the output format of the shape field. Case insensitive. Default value is `WKB`. Possible values are:
    - `SHP` Esri binary shape format.
    - `WKT` [Well known Text](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry).
    - `WKB` [Well Known Binary](https://postgis.net/docs/ST_AsBinary.html)
    - `GEOJSON` [GeoJSON](https://en.wikipedia.org/wiki/GeoJSON)
- `repair` An optional parameter to repair the read geometry. Possible values are:
    - `None` No repair.
    - `Esri` Apply Esri repair operator.
    - `OGC` Apply OGC repair operator.

### SQL API

```sql
CREATE TABLE gps
    USING com.esri.spark.shp
    OPTIONS
(
    path "data/gps.shp"
)
```

### Python API

```
df = spark.read \
    .format("shp") \
    .options(path="data/gps.shp", columns="atext,adate", format="GEOJSON") \
    .load() \
    .cache()
```

### Building From Source

This library is built using [Apache Maven](https://maven.apache.org/). To build the jar, execute the following command:

```shell script
mvn clean install                                       # Spark 3.5 / Scala 2.12
mvn -P spark-4.3,esri-geometry-github clean install     # Spark 4.3 / Scala 2.13
```

Name `esri-geometry-github` with any `-P`: Maven drops every `activeByDefault` profile once `-P` selects one, and that profile supplies the geometry API coordinates.

To read `s3a://` paths, add the `hadoop-aws` jar that matches your Spark's Hadoop version.

### Data

- Download the shapefile of [Metro Stations in DC](https://opendata.dc.gov/datasets/54018b7f06b943f2af278bbe415df1de_52)

### Create Conda Env

```bash
export ENV=spark-shp
conda remove --yes --all --name $ENV
conda create --yes --name $ENV python=3.6
source activate $ENV
conda install --yes --quiet -c conda-forge\
    jupyterlab\
    tqdm\
    future\
    matplotlib=3.1\
    gdal=2.4\
    pyproj=2.2\
    shapely=1.6\
    pyshp=2.1
```
