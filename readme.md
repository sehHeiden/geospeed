# Compare Speed for an overlay with intersection with different frameworks

GIS can change with current technology as Apache Arrow, and perhaps also Apache Spark und Apache Sedona.
Also in Pythonland Geopandas enhanced performance over time. In addition, I also wanted to try Dask und DuckDB.

Please contact me, when you read this and think I could have increased the code quality and speed.
I use some public data, which still widely use shape files in Germany.

## The Data used

First I load fhe ALKIS (register) building data for all counties in the state of Brandenburg.
All vector files are open data. The victor files are still offered as Shapefiles.
From the ALKIS Dataset of Brandenburg I used the buildings and the parcels (with land usage).
The files are stored per county!
The geometries have some errors which Geopandas auto-detects and fixes. 
I assume, that makes loading the data so slow.

## Task

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

## The Frameworks

### [Geopandas](https://geopandas.org/en/stable/index.html)

For me Geopandas is the goto solution for year now.
Sometimes with some extra code, some extra libs as pyogrio.

*Expectations*: Well, nothing special. It just works. Should load the data faster with [pyogrio](https://pyogrio.readthedocs.io/en/latest/).

*Observations*: Initially, loading the datasets takes about 87 s on my machine with am AMD Ryzen 5800X CPU.
Duration between different runs does vary slightly. But it is not really getting faster with pyogrio. 
I checked, it was not even installed before. So perhaps it defaults back to [fiona](https://fiona.readthedocs.io/en/latest/index.html)?
The Intersection takes about 190s. The python program takes about 10 GB of main memory, while running.

In the end I also tried to load and build the intersection per county and then just concat the results.
It's not faster, due to the spatial indexing.? RAM usage is initially much lower with about 3 GB.

With the reduced number of columns the durations are:

| Task           | Duration  pure Geopandas \s | Duration Geopandas & pyogrio \s | Duration Geopandas & pyogrio, per county \s |
|:---------------|----------------------------:|--------------------------------:|--------------------------------------------:|
| Loading Shape  |                       74.23 |                           58.67 |                                             |
| Intersection   |                      195.37 |                          182.98 |                                             |
| Saving Parquet |                       12.46 |                           11.48 |                                       12.38 |
| Overall        |                      282.06 |                          253.13 |                                      263.61 |

### [Dask-Geopandas](https://dask-geopandas.readthedocs.io/en/stable/)

*Expectations*: Partitioning the DataFrame should increase the number of used cores. Hence, reduce the compute time.

*Observations*: I open the shapefiles just as before with geopandas, but then convert to a Dask-Geopandas GeoDataFrames.
This all increases the loading time drastically! From about 60s to 160 s.
I try to overlay these GeoDataFrames, there is no overlay with Dask-Geoarray.

Instead, I try the map_partitions method. Left on a Dask-GeoDataFrame (the larger parcels dataset) and on the right
hand side the smaller houses GeoDataFrame. Having the larger dataset as Dask-GeoDataFrame increases the speed.
No, spatial swapping need as a spatial index is already used.
For the map_partitions I create function that wraps the overlay.

| Task           |       Duration Geopandas \s | Duration Dask-Geopandas \s |
|:---------------|----------------------------:|---------------------------:|
| Loading Shape  |                       58.67 |                      77.11 |
| Intersection   |                      182.98 |                      64.52 |
| Saving Parquet |                       11.48 |                      12.51 |
| Overall        |                      253.13 |                     154.13 |

This really uses all cores and a usage between 30 % and 95 % can be seen while the Overlay
is processed. This reduces the computation time to 33 % on this machine.

But three times faster, for 8 cores and 16 threads on the machine. Is not fully, what I expected.

### [DuckDB](https://duckdb.org/docs/extensions/spatial/overview)

DuckDB has a spatial extension. Although the csv/parquet file readers works great the
 asterix placeholder to load several file at once. 
This is not true for ST_Read for reading spatial data. Hence, I have like with the other frameworks.
Also, geoparquet is not supported for writing, hence I selected `FlatGeoBuf`.
There is no overlay I have to do all the steps by myself. So, there is a possibility that my solution is suboptimal.

Also writing the data does add a coordinate system. Nevertheless, the data can be opened with QGIS.
Due to the usage of FlatGeoBuf, file size and writing times are worse, than for geoparquet.
I could not save the data geopackage, to an error in sqlite3_exec, being unable to open the save tree.
The resulting FlatGeoBuf is huge.

**Expectation*: Not much, It's marked as faster than SQLite for DataAnalysis. Which holds true. 
But how does it compare to DataFrames, that are in the RAM, too. Should be faster, 
due to multicore usage. The memory layout benefits can not be much, as GeoPandas also uses Apache Arrow? 
*Observation*: The CPU usage here is high at first but reduces steadily.
For Dask the usage fluctuates.

The execution speed is much slower as with dask. Saving takes so long, that this is even as slow as normal
Geopandas.


| Task           |       Duration Geopandas \s | Duration DuckDB \s |
|:---------------|----------------------------:|-------------------:|
| Loading Shape  |                       74.23 |               7.87 |
| Intersection   |                      195.37 |              98.78 |
| Saving Parquet |                       12.46 |             104.39 |
| Overall        |                      282.06 |             275.03 |

## Apache Sedona

This one is a bit tricky for me. 
I want to get it working both with Scala and PySpark.

*Expectation*: Some loss due to virtualization with Docker. 
So the PySpark would be not as fast as Dask? But Scala could be faster?!