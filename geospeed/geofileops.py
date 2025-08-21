"""Test the speed of intersection with geofileops."""

import logging
import sys
import time
import warnings
from pathlib import Path

try:
    import geofileops as gfo  # type: ignore[import-untyped]
except ImportError as e:
    print(f"Warning: geofileops not available: {e}")
    print("To install geofileops, you need GDAL system dependencies.")
    print("See: https://github.com/theroggy/geofileops#installation")
    sys.exit(1)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
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
        # "geometry",
    ]
    parcels_cols = [
        "oid",
        "aktualit",
        "nutzart",
        "bez",
        "flstkennz",  # "geometry"
    ]

    # Use utils to get data directory
    import sys
    from pathlib import Path
    repo_root = Path(__file__).resolve().parents[1] 
    sys.path.insert(0, str(repo_root))
    from geospeed.utils import get_data_dir
    
    try:
        alkis_dir = get_data_dir()
        print(f"Using data directory: {alkis_dir}")
    except FileNotFoundError:
        print("No ALKIS data found - skipping geofileops benchmark")
        sys.exit(0)
        
    buildings_paths = list(alkis_dir.glob("*/GebauedeBauwerk.shp"))
    parcels_paths = list(alkis_dir.glob("*/NutzungFlurstueck.shp"))

    buildings_path = alkis_dir / "GebauedeBauwerk.gpkg"
    if not buildings_path.exists():
        for path in buildings_paths:
            gfo.copy_layer(
                src=path, dst=buildings_path, dst_layer=buildings_path.stem, append=True, create_spatial_index=False
            )
        # Try different API methods for spatial index creation
        try:
            gfo.create_spatial_index(buildings_path)
        except AttributeError:
            # Try alternative API
            try:
                gfo.add_spatial_index(buildings_path) 
            except AttributeError:
                print("Warning: No spatial index creation method found, continuing without index")

    parcels_path = alkis_dir / "NutzungFlurstueck.gpkg"
    if not parcels_path.exists():
        for path in parcels_paths:
            gfo.copy_layer(
                src=path, dst=parcels_path, dst_layer=parcels_path.stem, append=True, create_spatial_index=False
            )
        # Try different API methods for spatial index creation  
        try:
            gfo.create_spatial_index(parcels_path)
        except AttributeError:
            # Try alternative API
            try:
                gfo.add_spatial_index(parcels_path)
            except AttributeError:
                print("Warning: No spatial index creation method found, continuing without index")

    print(f"geofileops: Prepare data duration: {(time.time() - start):.0f} s.")

    start_intersection = time.time()
    buildings_with_parcels_path = alkis_dir / "buildings_with_parcels.gpkg"
    gfo.intersection(
        buildings_path,
        parcels_path,
        buildings_with_parcels_path,
        input1_columns=building_cols,
        input2_columns=parcels_cols,
    )
    print(f"geofileops: Load, intersection, save takes: {(time.time() - start_intersection):.0f} s.")

    print(f"geofileops: Total duration: {(time.time() - start):.0f} s.")
