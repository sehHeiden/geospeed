# Compare Speed for an overlay with intersection with different frameworks

GIS changes with current technology as Apache Arrow, Apache Spark und Apache Sedona.
Also in Pythonland Geopandas enhances over time. I also wanted to try Dask und DuckDB.

Please contact me, when you read this and think I could have increased the code quality and speed.
I use some public data, which still widely use shape files in Germany.

## [Geopandas](https://geopandas.org/en/stable/index.html)

For years the goto solution for me is geopandas, sometimes with some extra code, some extra libs the mentioned in their
documentation here and there.

### Task
First I load fhe ALKIS (register) building data for all counties in the state of Brandenburg.
All vector files are open data. Then I open the parcel data. Then I concat the buildings and parcels
and intersect both datasets and save as parquet file(Dask-Geopandas only writes Parquet) with a size of 391 MB
instead of the 1.57 GB a Geopackage file needs.

By the way, I select which columns to open, because later I will find out, that one column does which only contains `None`'s 
does not work will Arrow. Hence, I just don't use unimport columns form the start.

### Timing pure Geopandas

Initially, loading the datasets takes about 87 s on my machine with am AMD Ryzen 5800X CPU.
Duration between different runs my change somewhat. The Intersection takes about 190s and saving the 1.6 GB takes about 60 s.  
The python program takes about 14 GB of main memory, while running.

### Timing Geopandas with pyogrio and pyarrow

Reading and writing took much longer, than expected. So, it probably still uses [fiona](https://fiona.readthedocs.io/en/latest/index.html),
instead of [pyogrio](https://pyogrio.readthedocs.io/en/latest/).

Needs just as long, even while saving. Very strange. Normally I get:

 - a good speedup,
 - a message that shape does not support arrow.

So I assume, that there is something off and fiona is still used. Perhaps, because the files are ill formated. Things, happen all the time.

Therefore, I set which columns to load. Which resulted in a speedup of few percentages. Nothing dramatic.

### Timing Geopandas per County

I know, this is far from optimal, because I could do that county wise. 

Here we overlay with intersection per county and then concat only the result and save this. 
Therefore, RAM usage is initially much lower with about 3 GB.

In the end, this code is not faster, due to spatial indexing. It's working!

### The measurements

With reduced number of columns the durations are:

| Task           | Duration  pure Geopandas \s | Duration Geopandas & pyogrio \s | Duration Geopandas & pyogrio, per county \s |
|:---------------|----------------------------:|--------------------------------:|--------------------------------------------:|
| Loading Shape  |                       74.23 |                           58.67 |                                             |
| Intersection   |                      195.37 |                          182.98 |                                             |
| Saving Parquet |                       12.46 |                           11.48 |                                       12.38 |
| Overall        |                      282.06 |                          253.13 |                                      263.61 |

## Dask-Geopandas

I open the shapefiles just as before with geopandas, but then convert to a Dask-Geopandas GeoDataFrames.
This all increases the loading time drastically! From about 60s to 160 s.
I try to overlay these GeoDataFrames, but overlay does not exist with Dask-Geoarray.
They do have an GeoSeries.Intersection method. But that works on aligned vectors!

Instead, I try the map_partitions method. Left on a Dask-GeoDataFrame (the smaller housing dataset) and on the right
hand side the larger parcels GeoDataFrame.

| Task           |       Duration Geopandas \s | Duration Dask-Geopandas \s |
|:---------------|----------------------------:|---------------------------:|
| Loading Shape  |                       74.23 |                      73.98 |
| Intersection   |                      195.37 |                      87.79 |
| Saving Parquet |                       12.46 |                      12.04 |
| Overall        |                      282.06 |                     173.81 |


## DuckDB

DuckDB has spatial extension.