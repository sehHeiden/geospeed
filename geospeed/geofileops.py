"""Test the speed of intersection with geofileops."""

import logging
import os
import shutil
import subprocess
import sys
import time
import warnings
from pathlib import Path
from typing import NoReturn

import pandas as pd

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

    def build_gpkg(paths: list[Path], gpkg_path: Path, layer_name: str) -> None:
        """
        Create or append shapefiles into a single GeoPackage using ogr2ogr.

        Falls back to geopandas if ogr2ogr is not available.
        """
        if not paths:
            err = f"No input shapefiles for {layer_name}"
            raise FileNotFoundError(err)

        if gpkg_path.exists():
            return

        ogr2ogr = shutil.which("ogr2ogr")
        if ogr2ogr:
            print(f"Building {gpkg_path.name} with ogr2ogr...")
            # Speed up SQLite-backed writes (safe for CI ephemeral FS)
            env = os.environ.copy()
            env["OGR_SQLITE_SYNCHRONOUS"] = "OFF"
            # First file: create
            first = paths[0]
            subprocess.run(  # noqa: S603
                [
                    ogr2ogr,
                    "-f",
                    "GPKG",
                    str(gpkg_path),
                    str(first),
                    "-nln",
                    layer_name,
                    "-nlt",
                    "PROMOTE_TO_MULTI",
                ],
                check=True,
                text=True,
                env=env,
            )
            # Append remaining
            for shp in paths[1:]:
                subprocess.run(  # noqa: S603
                    [
                        ogr2ogr,
                        "-f",
                        "GPKG",
                        "-append",
                        str(gpkg_path),
                        str(shp),
                        "-nln",
                        layer_name,
                        "-nlt",
                        "PROMOTE_TO_MULTI",
                    ],
                    check=True,
                    text=True,
                    env=env,
                )
            return

        # Fallback: geopandas (slower, but portable)
        print(f"ogr2ogr not found; falling back to GeoPandas to build {gpkg_path.name}...")
        try:
            # Import geopandas via __import__ to avoid top-level import in function scope
            gpd = __import__("geopandas")  # type: ignore[import-not-found]
        except Exception as e:  # pragma: no cover - defensive
            print(f"GeoPandas not available to build {gpkg_path.name}: {e}")
            raise

        dfs = []
        for shp in paths:
            try:
                df = gpd.read_file(shp)
            except Exception as e:  # pragma: no cover - defensive
                print(f"Failed to read {shp}: {e}")
                raise
            dfs.append(df)
        if not dfs:
            err = f"No data read for {layer_name}"
            raise RuntimeError(err)
        out = pd.concat(dfs, ignore_index=True)
        out.to_file(gpkg_path, layer=layer_name, driver="GPKG")

    buildings_path = alkis_dir / "GebauedeBauwerk.gpkg"
    if not buildings_path.exists():
        print("Preparing buildings data...")
        try:
            build_gpkg(buildings_paths, buildings_path, buildings_path.stem)
        except (OSError, RuntimeError, subprocess.CalledProcessError) as e:
            print(f"Failed to prepare buildings data: {e}")
            sys.exit(1)
        print("Note: Skipping spatial index creation for buildings - proceeding without index")

    parcels_path = alkis_dir / "NutzungFlurstueck.gpkg"
    if not parcels_path.exists():
        print("Preparing parcels data...")
        try:
            build_gpkg(parcels_paths, parcels_path, parcels_path.stem)
        except (OSError, RuntimeError, subprocess.CalledProcessError) as e:
            print(f"Failed to prepare parcels data: {e}")
            sys.exit(1)
        print("Note: Skipping spatial index creation for parcels - proceeding without index")

    print(f"geofileops: Prepare data duration: {(time.time() - start):.0f} s.")

    start_intersection = time.time()
    buildings_with_parcels_path = alkis_dir / "buildings_with_parcels.gpkg"
    # Use geofileops for intersection, with version-tolerant fallback
    gfo_api = gfo.gfo if hasattr(gfo, "gfo") else gfo
    try:
        if hasattr(gfo_api, "intersection"):
            print("Running geofileops.intersection() ...")
            gfo_api.intersection(
                buildings_path,
                parcels_path,
                buildings_with_parcels_path,
                input1_columns=building_cols,
                input2_columns=parcels_cols,
            )
        elif hasattr(gfo_api, "overlay"):
            print("Running geofileops.overlay(operation='intersection') ...")
            gfo_api.overlay(
                input1=buildings_path,
                input2=parcels_path,
                out=buildings_with_parcels_path,
                operation="intersection",
                input1_columns=building_cols,
                input2_columns=parcels_cols,
            )
        else:
            err = "Neither 'intersection' nor 'overlay' method available in geofileops"
            _raise_geofileops_methods_error(err)
    except AttributeError as e:
        _handle_attribute_error(e, gfo, gfo_api)
    print(f"geofileops: Load, intersection, save takes: {(time.time() - start_intersection):.0f} s.")

    print(f"geofileops: Total duration: {(time.time() - start):.0f} s.")
