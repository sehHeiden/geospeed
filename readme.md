# GeoSpeed 🚀

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![CI/CD Pipeline](https://github.com/sehHeiden/geospeed/workflows/Benchmarks/badge.svg)](https://github.com/sehHeiden/geospeed/actions/workflows/benchmark.yml)
[![Coverage](https://codecov.io/gh/sehHeiden/geospeed/branch/master/graph/badge.svg)](https://codecov.io/gh/sehHeiden/geospeed)
[![Code Quality](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Package Manager](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Type Checking](https://img.shields.io/badge/type_checker-ty-blue.svg)](https://github.com/pydantic/ty)
[![Automated Testing](https://img.shields.io/badge/testing-automated-green.svg)](#testing)
[![Memory Profiling](https://img.shields.io/badge/profiling-psutil-orange.svg)](https://psutil.readthedocs.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Python GIS Performance Benchmarking, Updates with Automated CI/CD below**

A data-driven performance comparison of Python GIS frameworks featuring automated benchmarking, memory profiling.
This project benchmarks spatial overlay operations using real-world ALKIS building and parcel data from Brandenburg, Germany.

## 📊 Quick Results

| Framework | Duration | Peak RAM | Performance |
|-----------|----------|----------|--------------|
| **Dask-GeoPandas** | 169.6s ± 1.9s | 19 GB | 🏆 **Fastest** |
| **DuckDB (persistent)** | 233.4s ± 6.8s | 6.2 GB | 🧠 **Memory Efficient** |
| **GeoPandas (arrow)** | 264.6s ± 4.9s | 14 GB | ⚖️ **Balanced** |
| **DuckDB (memory)** | 271.5s ± 2.8s | 7 GB | 💾 **Low Memory** |
| **GeoPandas** | 287.5s ± 1.5s | 14 GB | 📚 **Baseline** |

## 🔬 About This Project

GIS technology is evolving rapidly with modern tools like Apache Arrow, improved pandas implementations, and distributed computing frameworks. This project evaluates how these technologies perform in real-world spatial analysis scenarios.

**Key Focus Areas:**
- 🏗️ **Spatial Overlay Operations** - Computationally intensive GIS tasks
- 📈 **Performance Benchmarking** - Execution time and memory usage
- 🔄 **Modern Technologies** - Latest versions of popular GIS libraries
- 📊 **Real-World Data** - Using actual German cadastral data (ALKIS)

> **Note:** This project uses public ALKIS data from Brandenburg, Germany, provided as shapefiles. If you have suggestions for improving code quality or performance, please feel free to contribute!

# The data

The benchmark uses official ALKIS (register) building data from [Brandenburg state](https://data.geobasis-bb.de/geobasis/daten/alkis/Vektordaten/shape/).
All vector files are open data provided as shapefiles by the Brandenburg government.
From the ALKIS dataset I use buildings and parcels (with land use) for two districts: Brandenburg City and Potsdam.

> **📥 Data Download**: To avoid GitHub LFS bandwidth limits, ALKIS data is downloaded directly from the official source:
> - **Linux/macOS/WSL**: `./scripts/download_alkis_data.sh`
> - **Windows PowerShell**: `scripts/download_alkis_data.ps1`
> - **Configuration**: Edit `scripts/alkis_config.txt` to change districts/layers
> - **CI/CD**: Automatically downloaded and cached, uses Ubuntu GDAL + micromamba
> - **Source**: https://data.geobasis-bb.de/geobasis/daten/alkis/Vektordaten/shape/
> - **Size**: 39M compressed → ~289M extracted
The geometries have some errors, which GeoPandas automatically detects and fixes.
Some files cannot be opened with the [fiona](https://fiona.readthedocs.io/en/latest/index.html) library due to
multiple geometry columns, so we use the newer default: pyogrio.

![SansSouci park and palaces in Potsdam, the capital of Brandenburg](/graphics/sanssouci_park.png)

# Task

1) Open the datasets and concatenate the counties.
2) Create an intersection overlay
3) Save the data, if possible, as a geoparquet file.

Why did I choose this task?
I think the overlay is one of the more computationally intensive tasks in GIS. I may write articles about other tasks later.
In [Geopandas](https://github.com/geopandas/geopandas/blob/main/geopandas/tools/overlay.py)
uses a spatial index, then calculates an intersection and joins the original data to the intersection. 

We save it as a geoparquet file, because that's the only format Dask-GeoPandas can write to.
In addition, the result (with good compression) is small (391 MB) compared to what Geopackage (1.57 GB) needs.

By the way, I choose which columns to open in Geopandas, because I will find out later that one column contains only `none`.
So I just don't use unimportant columns from the beginning.

# The hardware

I ran the speed test on a WIN10 PV with a Ryzen 7 5800X with 48 GB of RAM. 
The final runs are done with Hyperfine and 3 warm-up runs and the default 10 runs, 5 for DuckDB[^1].

[^1]: While using hyperfine on the DuckDB code, I found that a file is created in the temp folder for each output. 
These files have an uuid part and are never deleted, eg: `buildings_with_parcels_1976_2_temp.fgb`.
This seems to be a real bug in DuckDB.
So while profiling with hyperfine, the temp files are written to disk until the main drive is full.

The memory usage tests are done on a laptop with Ryzen 7 4800 and 32 GB of RAM running TuxedoOS.
The reason for this is that RAM usage is only fully recorded under Linux.

# The Frameworks

## [Geopandas](https://geopandas.org/en/stable/index.html)

For me, Geopandas has been the goto solution for years.
Sometimes with some extra code, some extra libs like pyogrio.

*Expectations: Well, nothing special. It just works. Should load faster with [pyogrio](https://pyogrio.readthedocs.io/en/latest/).

*Observations: Initially, loading the data takes about 75 to 80 seconds on my machine with an AMD Ryzen 5800X CPU.
It's a bit faster when using arrow by about 15 seconds.
 It got a bit slower when dropping the duplicates (on the district borders) by there `oid`. 

In the end I also tried to load and build the intersection per county and then just concatenate the results.
It's not faster because of the spatial indexing... The RAM usage is much lower at about 3 GB.

With the reduced number of columns, the running times are:

| Task         | Geopandas \s | Geopandas & arrow \s | Geopandas & pyogrio, per county \s |
|:-------------|-------------:|---------------------:|-----------------------------------:|
| Loading form |           74 |                   59 |                                    |
| Intersection |          204 |                  181 |                                    |
| Parquet      |           11 |                   11 |                                 12 |
| Total        |          290 |                  251 |                                264 |

We have saved 3,620,994 polygons.

## [Dask-Geopandas](https://dask-geopandas.readthedocs.io/en/stable/)


*Expectations:* Partitioning the DataFrame should increase the number of cores used. This should reduce the computation time.

*Observations:* I open the shapefiles as before with Geopandas, but then convert them to a Dask-Geopandas GeoDataFrame.
All this increases the loading time a bit from about 60 s to 76 s. It's not much because I don't do the spatial partioning!

Finally, I try the map_partitions method. On the left a Dask GeoDataFrame (the larger parcels dataset) and on the right the smaller GeoDataFrame on the right. Having the larger dataset as the Dask-GeoDataFrame increases speed.
No, spatial swapping is not necessary as the spatial index is already used.
For the map_partitions I create a function that wraps the overlay. This creates a single duplicate.

| Task         | Geopandas \s | Dask-Geopandas \s |
|:-------------|-------------:|------------------:|
| Loading form |           59 |                76 |
| Intersection |          181 |                62 |
| Parquet      |           11 |                12 |
| Total        |          251 |               151 |

This really does use all the cores, and you can see a usage between 30% and 95% while the Overlay
is being processed. This reduces the computing time to 33% on this machine.

But three times faster, for 8 cores and 16 threads on the machine. Not quite what I expected.

## [DuckDB](https://duckdb.org/docs/extensions/spatial/overview)

DuckDB has a spatial extension. Although the csv/parquet file readers work well, the
 tokens are to load multiple files at once. 
But this is not possible with ST_Read for reading spatial data. So I use pathlib as with the other frameworks.
Also, geoparquet is not supported for writing. So I chose `FlatGeobuf` as the geopackage could not be saved.
There is no overlay, I have to do all the steps myself. So there is a possibility that my solution is suboptimal.

Writing the data also adds a coordinate system. However, the data can be opened with QGIS.
By using FlatGeoBuf, the file size and write times are worse than for geoparquet.
I was unable to save the data geopackage due to an error in sqlite3_exec, which was unable to open the save tree.
The resulting FlatGeoBuf is huge.

*expectation*: Not much, it's marked as faster than SQLite for DataAnalysis. Which is true. 
But how does it compare to DataFrames, which are also in RAM? Should be faster, 
due to multicore usage. The memory layout benefits cannot be much, as GeoPandas also uses Apache Arrow? 
*Observation: CPU usage is high at first but drops steadily.
For Dask the usage fluctuates. I suspect this is due to index usage. The ST_Intersects operation uses the index, ST_Intersection does not.

The execution speed is much slower than for Dask. Saving takes so long that it is even as slow as normal geopandas. 
Using the database in persistent mode (giving the connection a filename) increases the execution time.
Loading takes 70% longer, but we eliminate the need to save the data. Yes, I could load the data into a DataFrame and save it, but then it is no longer a full GeoPandas DuckDB comparison.
The saved database is actually a bit smaller than the FlatGeoBuf file.
The comparison between DuckDB and Geopandas (with arrow) in speed is

| Task           | Geopandas \s | DuckDB (Memory) \s | DuckDB (db-file) \s |
|:---------------|-------------:|-------------------:|--------------------:|
| Loading Shape  |           59 |                 71 |                 120 |
| Intersection   |          181 |                 96 |                  92 |
| Saving         |           11 |                 93 |                 --- |
| Overall        |          251 |                261 |                 212 |
| Polygon Count  |      3620994 |            3619033 |                 --- |

DuckDB has a lower count in returned Polygons, but I assume that these are in included in the collections.

## [Apache Sedona](https://sedona.apache.org/latest/) with PySpark

*Expectations: Some loss due to virtualization with Docker. 
So PySpark would not be as fast as Dask?

Although the code is conceptually very similar to the database version. It is an interesting technology.
I started with the Sedona container as a docker-compose file. This created a local Spark instance with Sedona and Jupyter notebook.

The shapefiles can be loaded with a placeholder. No iteration (in code) is required.
But we need to validate the geometry with ST_MakeValid. Otherwise, I get a Java error message which is really long.
Which makes it hard to understand, at least if you are not used to it.
You can use SQL syntax on the DataFrames, or you can use message chaining methods.
I started with SQL code (which is more universal), but it contains long strings in your code.
Once everything was working, I switched to method chaining. Which in my eyes looks better, more functional.
This flexibility is a plus.

So far the code is lazy. A show method only executes on the row it will show, counting on all rows.
Lazy execution can lead to double execution, so I remove all count methods.
The slowest part seems to be writing. But differentiating the timing is challenging due to the lazy execution.
The data is saved as a geoqarquet file with 3618339 polygons, the size was about 320 MB with snappy compression and 250 MB with ZSTD.
Saving as a single parquet file takes about 158 seconds.
I would have liked to use more containers on a single device and let them talk to each other to get multiple workers and a master to see how much multi-node
reduced the computation time further. 

But that did not seem so trivial (please prove me wrong)

# Overall speed comparison

For the overall speed and memory usage comparison, I exclude Apache-Sedona as it is running in a Docker container (for now).

The previous timings were based on warmed runs but single executions. Here we use 10 warmed runs to get a better picture.
We need the warming because we use the same input data, so a GeoPandas run would also warm up the Dask GeoPandas run, and so on.
Without warming GeoPandas, this would be even slower.
These execution times must always be slower than the previous ones because they include loading the Python interpreter and all libraries. and all libraries.

Without using arrow for input data. GeoPandas took 287.519 s ± 1.532 s to open, overlay, and save.
The overall variation will be small.

Opening the files with the `use_arrow` option reduces the computation time by about 8%. The execution takes: 264.590 s ± 4.891 s.

With dask, the speed decreases slightly. The addition of multicore reduces the total execution time by a third. On the other hand,
I still have a good part (loading the data) that is limited to single core. So we end up with an execution time of 169.577 s ± 1.882 s.

For DuckDB with in-memory (and saving to FlatGeoBuf) the execution time is 271.455 s ± 2.790 s, and when persisting and not writing the final result it takes 233.427 s ± 6.805 s. 

# Total Memory Usage Comparison

The question here is whether it is necessary to hold all the data in RAM at the same time, or whether a good strategy can reduce RAM usage.
To hold only the final result and partitions of the input data.

The input data accounts for about 8 GB of RAM usage. This is the plateau in RAM usage of the Geopandas program. RAM usage peaks at just under 14 GB.

![RAM usage in Geopandas](./graphics/geopandas_ram.png)

Dask seems to need several copies of the input data. We have a first plateau at about 8 GB and a
at about 12 GB. The RAM usage peaks at about 19 GB.

![RAM usage in Geopandas](./graphics/dask_geopandas_ram.png)

The memory layout in DUCKDB greatly reduces the peak memory usage. I can also use a view 
for the final result, which adds up to even more savings.
Frankly, at only about 7 GB, the peak RAM usage is smaller than the input data in GeoPandas.
The input data alone uses about 3.5 GB.

![RAM usage in GeoPandas](./graphics/duckdb_memory_ram.png)

The persistence of the database does not change much, the input data seems to be even smaller at 2.5 GB.
The top RAM usage is also reduced to about 6.2 GB.
![RAM usage in Geopandas](./graphics/duckdb_persisted_ram.png)

# Conclusion

| Package             | Total duration \s | Top RAM usage \GB |
|:--------------------|------------------:|------------------:|
| Geopandas           |       287.5 ± 1.5 |                   |
| Geopandas (arrow)   |       264.6 ± 4.9 |                14 |
| Dask-Geopandas      |       169.6 ± 1.9 |                19 |
| DuckDB (in memory)  |       271.5 ± 2.8 |                 7 |
| DuckDB (persistent) |       233.4 ± 6.8 |               6.2 |


The intersection itself has a speedup S of about three for Dask-GeoDataFrames and two for DuckDB compared to GeoPandas.
This is despite using eight cores with hyper-threading. 
I suspect that DuckDB is slower here because intersection does not use the spatial index, but intersection does.
When we are able to use multiple cores, loading the data becomes a relatively long part of the total execution time.
Either a distribution in geoparquet or loading each file in a separate thread could help.

For Apache-Sedona we can only compare the total execution time, and this seems to be on par with Dask-GeoPandas.

If low memory usage is important, DUCKDB is an option. So either on systems with low memory or with huge amounts of data.
To avoid using swap. Opening shapefiles with DuckDB is slower than with GeoPandas. 
So far I cannot recommend using DuckDB for spatial tasks, as the number of supported file formats is limited, and although supported, I was not able to save to GeoPackages.
Also, DuckDB does not support raster files. 

If you already have a Spark cluster, Sedona may be a valid option. So far Dask is the fastest solution but uses a huge amount of additional memory.
Maybe one day I can recommend DuckDB instead.

# Automated Benchmark Results

> ⚠️ **Note:** The automated benchmarks use a **smaller test dataset** compared to the full Brandenburg ALKIS data used in the detailed analysis above. These results are primarily for CI/CD validation and relative performance comparison.

<!-- BENCHMARK_RESULTS_START -->

**Last updated**: 2025-12-29T21:34:28Z  
**Python**: 3.13.7  
**Dataset**: Test subset (significantly smaller than the full Brandenburg dataset)

| Framework | Status | Duration | Peak RAM | Notes |
|-----------|--------|----------|----------|-------|
| GeoPandas | ✅ | 9.3s | 737 MB | Baseline performance |
| Dask-GeoPandas | ✅ | 7.1s | 881 MB | ~24% faster than GeoPandas |
| DuckDB | ✅ | 8.1s | 516 MB | Lowest memory usage |
| GeoPandas (county-wise) | ✅ | 6.8s | 606 MB |  |
| geofileops | ✅ | 7.7s | 1.2 GB |  |
| Apache Sedona (PySpark) | ✅ | 45.3s | 1.8 GB |  |

<!-- BENCHMARK_RESULTS_END -->

*Automated results are updated by CI/CD pipeline and use a smaller test dataset for validation purposes. For comprehensive performance analysis with full-scale data, see the detailed benchmarks above.*
