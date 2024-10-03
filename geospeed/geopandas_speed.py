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

buildings_path = list(Path("./ALKIS").glob("./*/GebauedeBauwerk.shp"))
parcels_path = list(Path("./ALKIS").glob("./*/NutzungFlurstueck.shp"))

buildings_gdf = gpd.GeoDataFrame(
    pd.concat([gpd.read_file(x, columns=building_cols, engine="pyogrio", use_arrow=False) for x in buildings_path])
)
buildings_gdf = buildings_gdf.drop_duplicates(subset="oid", keep="first")

parcels_gdf = gpd.GeoDataFrame(
    pd.concat([gpd.read_file(x, columns=parcels_cols, engine="pyogrio", use_arrow=False) for x in parcels_path])
)
parcels_gdf = parcels_gdf.drop_duplicates(subset="oid", keep="first")
print(f"Geopandas: Loading data duration: {(time.time() - start):.0f} s.")

start_intersection = time.time()
buildings_with_parcels = gpd.overlay(buildings_gdf, parcels_gdf, how="intersection", keep_geom_type=True)
print(f"Geopandas: Intersection takes: {(time.time() - start_intersection):.0f} s.")

start_saving = time.time()
buildings_with_parcels.to_parquet("buildings_with_parcels.geoparquet")
print(f"Geopandas: Saving takes: {(time.time() - start_saving):.0f} s.")

print(f"Geopandas: Total duration: {(time.time() - start):.0f} s.")
