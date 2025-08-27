# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

This is a GIS performance benchmarking project that compares the speed and memory usage of different modern GIS tools for spatial overlay operations. The project specifically tests intersection operations using building and parcel data from Brandenburg, Germany (ALKIS dataset) across multiple frameworks:

- **GeoPandas** - Traditional Python GIS library
- **Dask-GeoPandas** - Distributed computing version of GeoPandas
- **DuckDB** - In-memory analytical database with spatial extensions
- **Apache Sedona** - Distributed spatial computing engine with PySpark
- **geofileops** - Optional GDAL-based library (requires system GDAL installation)

## Development Commands

### Environment Setup
```bash
# Install dependencies using uv
uv sync

# Install with development dependencies (includes ruff and mypy)
uv sync --group dev
```

### Code Quality and Formatting
```bash
# Format code with ruff (always format before committing)
uv run ruff format .

# Lint code with ruff
uv run ruff check .

# Type checking with mypy (though user prefers ty for type checking)
uv run mypy geospeed/
```

### Running Benchmarks
```bash
# Run individual framework benchmarks
uv run python geospeed/geopandas_speed.py
uv run python geospeed/dask_geopandas_speed.py  
uv run python geospeed/duckdb_speed.py
uv run python geospeed/geopandas_speed_county_wise.py

# Run Apache Sedona benchmark (requires Docker)
docker-compose up -d
uv run python geospeed/sedona_pyspark.py

# Run Apache Sedona standalone (CI version, no Docker)
uv run python geospeed/sedona_pyspark_ci.py

# Optional: Run geofileops benchmark (requires system GDAL installation)
# uv run python geospeed/geofileops.py

# CI/CD: Run all benchmarks and collect results
uv run python scripts/benchmarks.py
uv run python scripts/update_readme.py
```

### Performance Profiling
```bash
# Memory profiling (requires Linux for full accuracy)
uv run python -m memory_profiler geospeed/geopandas_speed.py

# Time benchmarking with hyperfine
hyperfine --warmup 3 --runs 10 "uv run python geospeed/geopandas_speed.py"
```

## Code Architecture

### Data Processing Pipeline
Each benchmark script follows a consistent three-phase pattern:
1. **Loading Phase**: Read shapefiles from `./ALKIS/*/` directories, drop duplicates by OID
2. **Intersection Phase**: Perform spatial overlay intersection between buildings and parcels
3. **Saving Phase**: Export results as geoparquet files

### Framework-Specific Approaches

**GeoPandas (`geopandas_speed.py`)**:
- Uses `pyogrio` engine with optional Arrow backend for faster I/O
- Processes all data in memory at once
- Simple `gpd.overlay()` function for intersection

**Dask-GeoPandas (`dask_geopandas_speed.py`)**:
- Converts to Dask GeoDataFrame with 14 partitions
- Uses `map_partitions()` with helper function `overlay_partitions()`
- Leverages multiple CPU cores for parallel processing

**DuckDB (`duckdb_speed.py`)**:
- Uses spatial extension with prepared statements for bulk loading
- Creates spatial indexes (R-Tree) for performance
- Manual intersection using `ST_Intersects()` and `ST_Intersection()`
- Supports both in-memory and persistent database modes

**Apache Sedona (`sedona_pyspark.py`)**:
- Runs in Docker container with Spark cluster
- Uses method chaining over SQL strings for better code structure
- Lazy evaluation with final coalesce for single output file
- Requires geometry validation with `ST_MakeValid()`

**Apache Sedona CI (`sedona_pyspark_ci.py`)**:
- Standalone PySpark in local mode without Docker
- Automatic Java environment detection and setup
- Reduced memory settings for CI constraints (4GB vs 12GB)
- Downloads Sedona JARs automatically via spark.jars.packages

### Key Design Patterns

- **Column Selection**: Each script defines specific column lists to minimize memory usage
- **Duplicate Handling**: All frameworks drop duplicates by OID to handle county border overlaps
- **Geometry Validation**: DuckDB and Sedona explicitly validate geometries before processing
- **Error Handling**: Warnings are filtered out; robust file path handling with pathlib
- **Timing**: Consistent timing patterns with separate measurement of load/intersection/save phases

### Data Requirements

- Input data expected in `./ALKIS/*/` directory structure
- Shapefiles: `GebauedeBauwerk.shp` (buildings) and `NutzungFlurstueck.shp` (parcels)
- Output format: GeoParquet with ZSTD compression
- Coordinate system: ETRS89 / UTM zone 33N (EPSG:25833)

### Performance Characteristics

Based on benchmarking results (Ryzen 7 5800X, 48GB RAM):
- **Dask-GeoPandas**: Fastest execution (~170s), highest memory usage (~19GB)
- **DuckDB**: Lowest memory usage (~7GB), moderate speed (~270s) 
- **GeoPandas**: Baseline performance (~290s with pyogrio, ~14GB RAM)
- **Apache Sedona**: Comparable to Dask but requires container overhead

## CI/CD Automation

### Automated Benchmarks
The repository includes a GitHub Actions workflow (`.github/workflows/benchmark.yml`) that:
- Runs benchmarks automatically on push to main/master branches
- Schedules monthly benchmark runs (first day of each month at 03:00 UTC)
- Can be triggered manually via workflow_dispatch
- Uses Nix to install the latest GDAL and Java dependencies
- Updates the README with latest results automatically

### Scripts
- **`scripts/benchmarks.py`**: Orchestrates all benchmark runs, collects timings into `benchmarks/latest.json`
- **`scripts/update_readme.py`**: Updates README.md with results table between markdown markers
- Gracefully handles missing data (ALKIS directory) by skipping benchmarks
- Results are committed back to the repository automatically

### Results Format
Benchmark results are stored in `benchmarks/latest.json` with:
```json
{
  "meta": {
    "timestamp": "2024-01-01T03:00:00Z",
    "python": "3.12.11",
    "data_dir": "/path/to/ALKIS"
  },
  "runs": {
    "geopandas": {
      "status": "ok",
      "duration_sec": 264.5,
      "exit_code": 0
    }
  }
}
```
