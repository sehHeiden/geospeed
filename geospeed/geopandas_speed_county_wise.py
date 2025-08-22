"""Speed with geopandas for overlaying on each county."""

import time
import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd

try:
    from .utils import get_data_dir
except ImportError:
    # Handle when run as standalone script
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from geospeed.utils import get_data_dir

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

data_dir = get_data_dir()
buildings_with_parcels = []
for directory in data_dir.iterdir():
    # Only process directories, skip files
    if not directory.is_dir():
        continue
    buildings_path = directory / "GebauedeBauwerk.shp"
    parcels_path = directory / "NutzungFlurstueck.shp"
    # Skip if shapefile doesn't exist
    if not buildings_path.exists() or not parcels_path.exists():
        continue
    buildings_gdf = gpd.read_file(buildings_path, engine="pyogrio", use_arrow=True, columns=building_cols)
    parcels_gdf = gpd.read_file(parcels_path, engine="pyogrio", use_arrow=True, columns=parcels_cols)

    buildings_with_parcels.append(gpd.overlay(buildings_gdf, parcels_gdf, how="intersection", keep_geom_type=True))

buildings_with_parcels_gdf = gpd.GeoDataFrame(pd.concat(buildings_with_parcels))

start_saving = time.time()
buildings_with_parcels_gdf.to_parquet(Path("buildings_with_parcels.geoparquet"))
print(f"Geopandas: Saving takes: {(time.time() - start_saving):.2f} s.")
print(f"Geopandas: Total duration: {(time.time() - start):.2f} s.")
