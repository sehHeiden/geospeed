

GIS can change with current technology as Apache Arrow, and perhaps also Apache Spark und Apache Sedona.
Also in Pythonland Geopandas enhanced performance over time. In addition, I also wanted to try Dask und DuckDB.

Please contact me, when you read this and think I could have increased the code quality and speed.
I use some public data, which still widely use shape files in Germany.

# The Data

First I loaded fhe ALKIS (register) building data for [all counties in the state of Brandenburg](https://data.geobasis-bb.de/geobasis/daten/alkis/Vektordaten/shape/).
All vector files are open data. The victor files are still offered as Shapefiles.
From the ALKIS Dataset of Brandenburg I used the buildings and the parcels (with land usage).
The files are stored per county!
The geometries have some errors which Geopandas auto-detects and fixes.
In addition, some file can not be opened with the [fiona](https://fiona.readthedocs.io/en/latest/index.html) library from Geopandas, with the error message
of multiple  geometry columns. Hence, we always use de new default: pyogrio.

# Task

1) Open the datasets and concat the counties.
2) Create an overlay with intersection
3) Save the data se, when possible as geoparquet file.

Why did select this task?
I consider the overlay is one of the more compute heavy tasks in GIS. 
In [Geopandas](https://github.com/geopandas/geopandas/blob/main/geopandas/tools/overlay.py)
this uses a spatial index, then computes an intersection and joins the original data to the intersection. 

We close the saving as Geoparquet file, as it's the only format Dask-GeoPandas can write to.
In addition, the result (with snappy compression) is small 391 MB, compared to what Geopackage (1.57 GB) needs.

By the way, I select which columns to open in Geopandas, because later I will find out, that one column does only contain `None`'s.
Hence, I just don't use not important columns form the start.

# The Frameworks

## [Geopandas](https://geopandas.org/en/stable/index.html)

### Speed

For me Geopandas is the goto solution for year now.
Sometimes with some extra code, some extra libs as pyogrio.

*Expectations*: Well, nothing special. It just works. Should load the data faster with [pyogrio](https://pyogrio.readthedocs.io/en/latest/).

*Observations*: Initially, loading the datasets takes about 75 to 80 s on my machine with am AMD Ryzen 5800X CPU.
Duration between different runs does vary slightly.It's somewhat faster when using arrow by about 15 s.
 It got a bit slower, wehen dropping the duplicates (on the county borders) by there `oid`. 

In the end I also tried to load and build the intersection per county and then just concat the results.
It's not faster, due to the spatial indexing.? RAM usage is initially much lower with about 3 GB.

With the reduced number of columns the durations are:

| Task           | Geopandas \s | Geopandas & arrow \s | Geopandas & pyogrio, per county \s |
|:---------------|-------------:|---------------------:|-----------------------------------:|
| Loading Shape  |           74 |                   59 |                                    |
| Intersection   |          204 |                  181 |                                    |
| Saving Parquet |           11 |                   11 |                                 12 |
| Overall        |          290 |                  251 |                                264 |

We save 3.620.994 polygons.

### Memory Usage

## [Dask-Geopandas](https://dask-geopandas.readthedocs.io/en/stable/)

### Speed

*Expectations*: Partitioning the DataFrame should increase the number of used cores. Hence, reduce the compute time.

*Observations*: I open the shapefiles just as before with geopandas, but then convert to a Dask-Geopandas GeoDataFrames.
This all increases the loading time somewhat From about 60s to 76 s. It's not much because I do not create the spartial
partioning!

Finally, I try the map_partitions method. Left on a Dask-GeoDataFrame (the larger parcels dataset) and on the right
hand side the smaller houses GeoDataFrame. Having the larger dataset as Dask-GeoDataFrame increases the speed.
No, spatial swapping need as a spatial index is already used.
For the map_partitions I create function that wraps the overlay. This created a single duplicate.

| Task           | Geopandas \s | Dask-Geopandas \s |
|:---------------|-------------:|------------------:|
| Loading Shape  |           59 |                76 |
| Intersection   |          181 |                62 |
| Saving Parquet |           11 |                12 |
| Overall        |          251 |               151 |

This really uses all cores and a usage between 30 % and 95 % can be seen while the Overlay
is processed. This reduces the computation time to 33 % on this machine.

But three times faster, for 8 cores and 16 threads on the machine. Is not fully, what I expected.

### Memory Usage

## [DuckDB](https://duckdb.org/docs/extensions/spatial/overview)

### Speed

DuckDB has a spatial extension. Although the csv/parquet file readers works great the
 asterix placeholder to load several file at once. 
But it not possible with ST_Read for reading spatial data. Hence, I use pathlib like with the other frameworks.
Also, geoparquet is not supported for writing. Therefore, I selected `FlatGeobuf`, as Geopackage could not be saved.
There is no overlay I have to do all the steps by myself. So, there is a possibility that my solution is suboptimal.

Also writing the data does add a coordinate system. Nevertheless, the data can be opened with QGIS.
Due to the usage of FlatGeoBuf, file size and writing times are worse, than for geoparquet.
I could not save the data geopackage, to an error in sqlite3_exec, being unable to open the save tree.
The resulting FlatGeoBuf is huge.

*Expectation*: Not much, It's marked as faster than SQLite for DataAnalysis. Which holds true. 
But how does it compare to DataFrames, that are in the RAM, too. Should be faster, 
due to multicore usage. The memory layout benefits can not be much, as GeoPandas also uses Apache Arrow? 
*Observation*: The CPU usage here is high at first but reduces steadily.
For Dask the usage fluctuates. I assume, that due to the index usage. The operation ST_Intersects

The execution speed is much slower as with dask. Saving takes so long, that this is even as slow as normal
Geopandas. 
When use the database in persistent mode (giving the connection a file name), the execution time increases.
The loading takes 70 % longer, but we remove the need for saving the data. Yes, I could load the data into
a DataFrame and save this, but then its name a full GeoPandas DuckDB comparison anymore.
The saved database is even a bit smaller than the FlatGeoBuf-File.
The comparison between the duckDB and Geopandas (with Arrow) in speed is:

| Task           | Geopandas \s | DuckDB (Memory) \s | DuckDB (db-file) \s |
|:---------------|-------------:|-------------------:|--------------------:|
| Loading Shape  |           59 |                 71 |                 120 |
| Intersection   |          181 |                 96 |                  92 |
| Saving         |           11 |                 93 |                 --- |
| Overall        |          251 |                261 |                 212 |
| Polygon Count  |      3620994 |            3619033 |                 ??? |

### Memory Usage

## Apache Sedona

This one is a bit tricky for me. 
I want to get it working both with Scala and PySpark.

### PySpark

*Expectation*: Some loss due to virtualization with Docker. 
So the PySpark would be not as fast as Dask? But Scala could be faster?!\

Although the code is conceptual very similar I to the database version. It is an interesting technique.
I started with the Sedona container as docker-compose file. This created a local Spark instance with Sedona and Jupyter notebook.
Writing to a notebook 

The shapefiles can be loaded with a wildcard. No iteration needed. But we need to fix the geometries with ST_MakeValid!
You can use SQL syntax on the DataFrames, or yoy can use methods.
I use SQL for the overlay and method chaining for the rest. That flexibility is a plus, in addition I like method chaining.

So far the code is lazily executed. A show methods, only executes on the row it will show, count on all rows.
Lazy execution might lead to double execution, hence I remove all count methods.
The slowest parts seams to be the writing. But the differentiation of the timing is hard, due to the lazy execution.
Data is saved as a geoqarquet file with 3618339 polygons, the size was about 320 MB with snappy compression and 250 MB with ZSTD.
Saving as a single parquet file takes about 158 seconds.

### Spark

## Compare Speed-Up and RAM

