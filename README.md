# Spark Shp

A library for parsing and querying Shapefile data with Apache Spark, for Spark SQL and DataFrames.

```
spark-shell --packages com.esri:spark-shp:XX
```

### Usage Example

```
df = spark.read \
    .format("com.esri.shp") \
    .options(path=path) \
    .load()
```

```
import io
from struct import pack, unpack, calcsize, error, Struct

shape=bytearray(b'\x01\x00\x00\x00\xfe\xd4x\xe9&q,@\x02+\x87\x16\xd9\xce\x13\xc0')
f = io.BytesIO(shape)
shapeType = unpack("<i", f.read(4))[0]
print(shapeType)
points = unpack("<2d", f.read(16))
for p in points:
    print(p)
```
