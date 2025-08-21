"""Shared utilities for geospeed benchmarks."""

from pathlib import Path


def get_data_dir() -> Path:
    """Auto-detect data directory: prefer CI dataset if available."""
    ci_dir = Path("./ALKIS_CI")
    full_dir = Path("./ALKIS")

    if ci_dir.exists() and any(ci_dir.glob("*/GebauedeBauwerk.shp")):
        print("Using CI dataset (ALKIS_CI)")
        return ci_dir
    if full_dir.exists():
        print("Using full dataset (ALKIS)")
        return full_dir
    no_alkis_data_msg = "No ALKIS data found in ./ALKIS_CI or ./ALKIS"
    raise FileNotFoundError(no_alkis_data_msg)


def get_file_paths() -> tuple[list[Path], list[Path]]:
    """Get building and parcel file paths from data directory."""
    data_dir = get_data_dir()
    buildings_paths = list(data_dir.glob("*/GebauedeBauwerk.shp"))
    parcels_paths = list(data_dir.glob("*/NutzungFlurstueck.shp"))

    if not buildings_paths or not parcels_paths:
        no_shapefiles_msg = "No shapefiles found in the data directory."
        raise FileNotFoundError(no_shapefiles_msg)

    return buildings_paths, parcels_paths
