"""Test the speed of overlay with dask."""

import time
import warnings
from pathlib import Path

import dask_geopandas as dpd
import geopandas as gpd
import pandas as pd


# Function to apply the overlay in each partition using GeoPandas
def overlay_partitions(part1: gpd.GeoDataFrame, part2: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Use helper function to calculate overlay with dask."""
    return gpd.overlay(part1, part2, how="intersection")


warnings.filterwarnings("ignore")

start = time.time()

buildings_path = list(Path("../ALKIS").glob("./*/GebauedeBauwerk.shp"))
parcels_path = list(Path("../ALKIS").glob("./*/NutzungFlurstueck.shp"))

buildings_gdf = gpd.GeoDataFrame(
    pd.concat([gpd.read_file(x, engine="pyogrio", use_arrow=True) for x in buildings_path])
)
parcels_gdf = gpd.GeoDataFrame(pd.concat([gpd.read_file(x, engine="pyogrio", use_arrow=True) for x in parcels_path]))
parcels_ddf = dpd.from_geopandas(parcels_gdf, npartitions=14)

print(f"Dask: Loading data duration: {(time.time() - start):.2f} s.")

start_intersection = time.time()
# Use Dask's map_partitions to apply the overlay function
buildings_with_parcels = parcels_ddf.map_partitions(overlay_partitions, buildings_gdf).compute()
print(f"Dask: Intersection takes: {(time.time() - start_intersection):.2f} s.")

start_saving = time.time()
buildings_with_parcels.to_parquet("buildings_with_parcels.geoparquet")
print(f"Dask: Saving takes: {(time.time() - start_saving):.2f} s.")

print(f"Dask: Total duration: {(time.time() - start):.2f} s.")
