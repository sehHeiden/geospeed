#!/usr/bin/env python
"""
Run the repository's benchmark scripts and collect timing results.

- Detects presence of input data in ./ALKIS (or via DATA_DIR env).
- Runs each benchmark script as a subprocess using the current interpreter.
- Records wall-clock durations and exit codes in benchmarks/latest.json.
- Skips gracefully if the data directory is missing.

Usage:
    uv run python scripts/benchmarks.py
"""

from __future__ import annotations

import contextlib
import json
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import psutil
else:
    try:
        import psutil
    except ImportError:
        print("psutil not available - RAM monitoring disabled")
        psutil = None

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from geospeed.utils import get_data_dir  # noqa: E402

DATA_DIR = get_data_dir()

RESULTS_DIR = REPO_ROOT / "benchmarks"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_FILE = RESULTS_DIR / "latest.json"

# Scripts to run: name -> path
SCRIPTS: list[tuple[str, Path]] = [
    ("geopandas", REPO_ROOT / "geospeed" / "geopandas_speed.py"),
    ("dask_geopandas", REPO_ROOT / "geospeed" / "dask_geopandas_speed.py"),
    ("duckdb", REPO_ROOT / "geospeed" / "duckdb_speed.py"),
    ("geopandas_county_wise", REPO_ROOT / "geospeed" / "geopandas_speed_county_wise.py"),
    ("geofileops", REPO_ROOT / "geospeed" / "geofileops.py"),  # optional; may fail if GDAL missing
    ("sedona_pyspark", REPO_ROOT / "geospeed" / "sedona_pyspark_ci.py"),  # CI version without Docker
]


def has_data(data_dir: Path) -> bool:
    """Check if the data directory contains expected shapefiles."""
    if not data_dir.exists():
        return False
    # Look for at least one expected shapefile
    return any(sub.is_file() for sub in data_dir.glob("*/GebauedeBauwerk.shp"))


def run_script(path: Path) -> tuple[int, float, str, float | None]:
    """Run a benchmark script and collect timing and memory information."""
    start = time.perf_counter()
    peak_mem_mb: float | None = None
    min_mem_free_mb: float | None = None

    # If psutil available, sample RSS of current process recursively
    stop_flag = False

    def sampler() -> None:
        nonlocal peak_mem_mb
        nonlocal min_mem_free_mb
        if psutil is None:
            return
        try:
            this_proc = psutil.Process()
            while not stop_flag:
                mem_free_mb = psutil.virtual_memory().available / (1024 * 1024)
                min_mem_free_mb = mem_free_mb if min_mem_free_mb is None else min(min_mem_free_mb, mem_free_mb)
                rss = this_proc.memory_info().rss
                # Include children
                for child in this_proc.children(recursive=True):
                    with contextlib.suppress(psutil.Error):
                        rss += child.memory_info().rss
                cur_mb = rss / (1024 * 1024)
                peak_mem_mb = cur_mb if peak_mem_mb is None or cur_mb > peak_mem_mb else peak_mem_mb
                time.sleep(0.01)
        except psutil.Error:
            pass

    t = threading.Thread(target=sampler, daemon=True)
    if psutil is not None:
        t.start()

    pre_mem_free_mb = psutil.virtual_memory().available / (1024 * 1024)
    try:
        proc = subprocess.run([sys.executable, str(path)], check=False, capture_output=True, text=True)  # noqa: S603
    except FileNotFoundError as e:
        duration = time.perf_counter() - start
        peak_mem_less_avail_mb = pre_mem_free_mb - min_mem_free_mb if min_mem_free_mb is not None else None
        return 127, duration, f"File not found: {e}", peak_mem_mb, peak_mem_less_avail_mb
    else:
        duration = time.perf_counter() - start
        peak_mem_less_avail_mb = pre_mem_free_mb - min_mem_free_mb if min_mem_free_mb is not None else None
        output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
        return proc.returncode, duration, output, peak_mem_mb, peak_mem_less_avail_mb
    finally:
        stop_flag = True
        if psutil is not None and t.is_alive():
            t.join(timeout=0.5)


def main() -> int:
    """Run all benchmark scripts and collect results."""
    results: dict[str, dict[str, object]] = {
        "meta": {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "python": sys.version.split()[0],
            "data_dir": str(DATA_DIR),
        },
        "runs": {},
    }

    if not has_data(DATA_DIR):
        results["meta"]["skipped"] = True
        results["meta"]["reason"] = f"No input data found under {DATA_DIR}."
        RESULTS_FILE.write_text(json.dumps(results, indent=2))
        print(results["meta"]["reason"])  # CI log hint
        return 0

    for name, path in SCRIPTS:
        if not path.exists():
            results["runs"][name] = {
                "status": "missing",
                "duration_sec": None,
                "exit_code": 127,
            }
            continue
        code, dur, out, peak_mem_mb, peak_mem_less_avail_mb = run_script(path)
        results["runs"][name] = {
            "status": "ok" if code == 0 else "error",
            "duration_sec": round(dur, 3),
            "exit_code": code,
        }
        # Add memory info if available
        if peak_mem_mb is not None:
            results["runs"][name]["peak_memory_mb"] = round(peak_mem_mb, 1)  # type: ignore[index]
        if peak_mem_less_avail_mb is not None:
            results["runs"][name]["peak_memory_less_available_mb"] = round(peak_mem_less_avail_mb, 1)  # type: ignore[index]
        # Keep a short log snippet in case of failure
        if code != 0:
            results["runs"][name]["log_tail"] = out.splitlines()[-20:]  # type: ignore[index]

    RESULTS_FILE.write_text(json.dumps(results, indent=2))
    print(f"Wrote results to {RESULTS_FILE}")
    print(json.dumps(results, indent=2))
    # Return 0 even on errors so CI can still update README
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
