"""Test the speed of overlay with geopandas."""

import time
import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd

warnings.filterwarnings("ignore")

start = time.time()

building_cols = [
    "oid",
    "aktualit",
    "gebnutzbez",
    "funktion",
    "anzahlgs",
    "gmdschl",
    "lagebeztxt",
    "geometry",
]
parcels_cols = ["oid", "aktualit", "nutzart", "bez", "flstkennz", "geometry"]

buildings_path = [
    x for directory in Path("../ALKIS").iterdir() for x in directory.iterdir() if x.name == "GebauedeBauwerk.shp"
]
parcels_path = [
    x for directory in Path("../ALKIS").iterdir() for x in directory.iterdir() if x.name == "NutzungFlurstueck.shp"
]

buildings_gdf = gpd.GeoDataFrame(
    pd.concat([gpd.read_file(x, columns=building_cols, engine="pyogrio", use_arrow=True) for x in buildings_path])
)
parcels_gdf = gpd.GeoDataFrame(
    pd.concat([gpd.read_file(x, columns=parcels_cols, engine="pyogrio", use_arrow=True) for x in parcels_path])
)
print(f"Geopandas: Loading data duration: {(time.time() - start):.2f} s.")

start_intersection = time.time()
buildings_with_parcels = gpd.overlay(buildings_gdf, parcels_gdf, how="intersection", keep_geom_type=True)
print(f"Geopandas: Intersection takes: {(time.time() - start_intersection):.2f} s.")

start_saving = time.time()
buildings_with_parcels.to_parquet("buildings_with_parcels.geoparquet")
print(f"Geopandas: Saving takes: {(time.time() - start_saving):.2f} s.")

print(f"Geopandas: Total duration: {(time.time() - start):.2f} s.")
