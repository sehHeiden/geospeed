"""Test the speed of intersection with geofileops."""

import logging
import sys
import time
import warnings
from pathlib import Path
from typing import NoReturn

try:
    # Remove current script directory from path to avoid circular import
    script_dir = str(Path(__file__).parent)
    if script_dir in sys.path:
        sys.path.remove(script_dir)

    import geofileops as gfo  # type: ignore[import-untyped]
except ImportError as e:
    print(f"Warning: geofileops not available: {e}")
    print("To install geofileops, you need GDAL system dependencies.")
    print("See: https://github.com/theroggy/geofileops#installation")
    sys.exit(1)

# Add import handling for standalone execution
try:
    from .utils import get_data_dir
except ImportError:
    # Handle when run as standalone script
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from geospeed.utils import get_data_dir


def _raise_geofileops_methods_error(msg: str) -> NoReturn:
    raise AttributeError(msg)


def _handle_attribute_error(e: AttributeError, gfo: object, gfo_api: object) -> None:
    available_methods = [method for method in dir(gfo) if not method.startswith("_")]
    if gfo_api != gfo:
        gfo_methods = [method for method in dir(gfo_api) if not method.startswith("_")]
        print(f"Error: geofileops API changed. Top-level methods: {available_methods}")
        print(f"Methods in gfo.gfo: {gfo_methods}")
    else:
        print(f"Error: geofileops API changed. Available methods: {available_methods}")
    print(f"Specific error: {e}")
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
        print("Preparing buildings data...")
        for path in buildings_paths:
            try:
                # Try different geofileops API functions based on version
                # Check if gfo has a sub-module or if methods are available directly
                gfo_api = gfo.gfo if hasattr(gfo, "gfo") else gfo

                if hasattr(gfo_api, "copy_layer"):
                    gfo_api.copy_layer(
                        src=path,
                        dst=buildings_path,
                        dst_layer=buildings_path.stem,
                        append=True,
                        create_spatial_index=False,
                    )
                elif hasattr(gfo_api, "append_to"):
                    # Alternative API in newer versions
                    gfo_api.append_to(src=path, dst=buildings_path, dst_layer=buildings_path.stem)
                elif hasattr(gfo_api, "copy"):
                    # Fallback: use copy function if available
                    gfo_api.copy(src=path, dst=buildings_path, dst_layer=buildings_path.stem)
                else:
                    msg = "Neither 'copy_layer', 'append_to', nor 'copy' methods are available in geofileops."
                    _raise_geofileops_methods_error(msg)
            except AttributeError as e:
                _handle_attribute_error(e, gfo, gfo_api)
        print("Note: Skipping spatial index creation for buildings - proceeding without index")

    parcels_path = alkis_dir / "NutzungFlurstueck.gpkg"
    if not parcels_path.exists():
        print("Preparing parcels data...")
        for path in parcels_paths:
            try:
                # Use the same API access pattern
                gfo_api = gfo.gfo if hasattr(gfo, "gfo") else gfo

                if hasattr(gfo_api, "copy_layer"):
                    gfo_api.copy_layer(
                        src=path, dst=parcels_path, dst_layer=parcels_path.stem, append=True, create_spatial_index=False
                    )
                elif hasattr(gfo_api, "append_to"):
                    gfo_api.append_to(src=path, dst=parcels_path, dst_layer=parcels_path.stem)
                elif hasattr(gfo_api, "copy"):
                    gfo_api.copy(src=path, dst=parcels_path, dst_layer=parcels_path.stem)
                else:
                    msg = "Neither 'copy_layer', 'append_to', nor 'copy' methods are available in geofileops."
                    _raise_geofileops_methods_error(msg)
            except AttributeError as e:
                _handle_attribute_error(e, gfo, gfo_api)
        print("Note: Skipping spatial index creation for parcels - proceeding without index")

    print(f"geofileops: Prepare data duration: {(time.time() - start):.0f} s.")

    start_intersection = time.time()
    buildings_with_parcels_path = alkis_dir / "buildings_with_parcels.gpkg"
    # Use the same API access pattern for intersection
    gfo_api = gfo.gfo if hasattr(gfo, "gfo") else gfo
    gfo_api.intersection(
        buildings_path,
        parcels_path,
        buildings_with_parcels_path,
        input1_columns=building_cols,
        input2_columns=parcels_cols,
    )
    print(f"geofileops: Load, intersection, save takes: {(time.time() - start_intersection):.0f} s.")

    print(f"geofileops: Total duration: {(time.time() - start):.0f} s.")
