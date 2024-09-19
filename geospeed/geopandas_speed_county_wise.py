"""Speed with geopandas for overlaying on each county."""
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

buildings_with_parcels = []
for directory in Path("../ALKIS").iterdir():
    buildings_path = directory / "GebauedeBauwerk.shp"
    parcels_path = directory / "NutzungFlurstueck.shp"
    buildings_gdf = gpd.read_file(buildings_path, engine="pyogrio", use_arrow=True, columns=building_cols)
    parcels_gdf = gpd.read_file(parcels_path, engine="pyogrio", use_arrow=True, columns=parcels_cols)

    buildings_with_parcels.append(gpd.overlay(buildings_gdf, parcels_gdf, how="intersection", keep_geom_type=True))

buildings_with_parcels_gdf = gpd.GeoDataFrame(pd.concat(buildings_with_parcels))

start_saving = time.time()
buildings_with_parcels_gdf.to_parquet("buildings_with_parcels.geoparquet")
print(f"Geopandas: Saving takes: {(time.time() - start_saving):.2f} s.")
print(f"Geopandas: Total duration: {(time.time() - start):.2f} s.")
