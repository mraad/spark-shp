# Shapefile Data Source for Apache Spark

A library for parsing and querying [shapefile](https://en.wikipedia.org/wiki/Shapefile) data with Apache Spark, for Spark SQL and DataFrames.

### Requirements

This library requires Spark 2.0+

### Using with Spark shell

```shell script
$SPARK_HOME/bin/spark-shell --packages com.esri:spark-shp:0.3
```

### Features

This package allows reading shapefiles in local or distributed filesystem as Spark DataFrames. When reading files the API accepts several options:

- `path` The location of shapefile(s). Similar to Spark can accept standard Hadoop globbing expressions.
- `shape` An optional name of the shape column. Default value is `shape`. 

### SQL API

```sql
CREATE TABLE gps
USING com.esri.spark.shp
OPTIONS (path "data/gps.shp")
```

### Python API

```
df = spark.read \
    .format("com.esri.spark.shp") \
    .options(path="data/gps.shp") \
    .load()
```

### Building From Source

This library is built using [Apache Maven](https://maven.apache.org/). To build the jar, execute the following command:

```shell script
mvn clean install
```

### Data

- Download the shapefile of [Metro Stations in DC](https://opendata.dc.gov/datasets/54018b7f06b943f2af278bbe415df1de_52)
