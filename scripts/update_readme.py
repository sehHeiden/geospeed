#!/usr/bin/env python
"""
Update README.md with the latest benchmark results.

Looks for benchmarks/latest.json and injects a table between:
<!-- BENCHMARK_RESULTS_START -->
<!-- BENCHMARK_RESULTS_END -->

Usage:
    uv run python scripts/update_readme.py
"""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
README_FILE = REPO_ROOT / "readme.md"
RESULTS_FILE = REPO_ROOT / "benchmarks" / "latest.json"

START_MARKER = "<!-- BENCHMARK_RESULTS_START -->"
END_MARKER = "<!-- BENCHMARK_RESULTS_END -->"

# Constants for formatting
SECONDS_PER_MINUTE = 60
MB_PER_GB = 1024


def format_duration(duration_sec: float | None) -> str:
    """Format duration in seconds to human-readable string."""
    if duration_sec is None:
        return "N/A"
    if duration_sec < SECONDS_PER_MINUTE:
        return f"{duration_sec:.1f}s"
    minutes = int(duration_sec // SECONDS_PER_MINUTE)
    seconds = duration_sec % SECONDS_PER_MINUTE
    return f"{minutes}m {seconds:.1f}s"


def format_memory(memory_mb: float | None) -> str:
    """Format memory usage in MB to human-readable string."""
    if memory_mb is None:
        return "N/A"
    if memory_mb < MB_PER_GB:
        return f"{memory_mb:.0f} MB"
    return f"{memory_mb / MB_PER_GB:.1f} GB"


def create_results_table(results: dict) -> str:
    """Create a Markdown table from benchmark results."""
    if "runs" not in results or not results["runs"]:
        return (
            "**No benchmark results available**\n\n*Run benchmarks with data in ./ALKIS directory to generate results.*"
        )

    # Check if benchmarks were skipped
    if results.get("meta", {}).get("skipped"):
        reason = results["meta"].get("reason", "Unknown reason")
        return f"**Benchmarks skipped**: {reason}"

    runs = results["runs"]
    timestamp = results.get("meta", {}).get("timestamp", "Unknown")
    python_version = results.get("meta", {}).get("python", "Unknown")

    lines = _create_table_header(timestamp, python_version)

    # Framework display names
    display_names = _get_framework_display_names()

    for name, run_data in runs.items():
        display_name = display_names.get(name, name)
        status = run_data.get("status", "unknown")
        duration = format_duration(run_data.get("duration_sec"))
        memory = format_memory(run_data.get("peak_memory_mb"))

        status_icon, notes = _get_status_and_notes(status, name, run_data, runs)
        lines.append(f"| {display_name} | {status_icon} | {duration} | {memory} | {notes} |")

    return "\n".join(lines)


def _create_table_header(timestamp: str, python_version: str) -> list[str]:
    """Create the header section of the result table."""
    return [
        f"**Last updated**: {timestamp}  ",
        f"**Python**: {python_version}  ",
        "**Dataset**: Test subset (significantly smaller than the full Brandenburg dataset)",
        "",
        "| Framework | Status | Duration | Peak RAM | Notes |",
        "|-----------|--------|----------|----------|-------|",
    ]


def _get_framework_display_names() -> dict[str, str]:
    """Get mapping of framework names to display names."""
    return {
        "geopandas": "GeoPandas",
        "dask_geopandas": "Dask-GeoPandas",
        "duckdb": "DuckDB",
        "geopandas_county_wise": "GeoPandas (county-wise)",
        "geofileops": "geofileops",
        "sedona_pyspark": "Apache Sedona (PySpark)",
    }


def _get_status_and_notes(status: str, name: str, run_data: dict, runs: dict) -> tuple[str, str]:
    """Get status icon and notes for a benchmark run."""
    if status == "ok":
        return _get_success_status_and_notes(name, run_data, runs)
    if status == "error":
        exit_code = run_data.get("exit_code", "?")
        return "❌", f"Exit code: {exit_code}"
    if status == "missing":
        return "⚠️", "Script not found"
    return "❓", f"Status: {status}"


def _get_success_status_and_notes(name: str, run_data: dict, runs: dict) -> tuple[str, str]:
    """Get status icon and notes for successful runs."""
    status_icon = "✅"

    if name == "geopandas":
        notes = "Baseline performance"
    elif name == "dask_geopandas":
        notes = _get_dask_performance_notes(run_data, runs)
    elif name == "duckdb":
        notes = "Lowest memory usage"
    else:
        notes = ""

    return status_icon, notes


def _get_dask_performance_notes(run_data: dict, runs: dict) -> str:
    """Get performance notes for Dask-GeoPandas compared to GeoPandas."""
    gp_time = runs.get("geopandas", {}).get("duration_sec")
    if gp_time and run_data.get("duration_sec"):
        speedup = (gp_time - run_data["duration_sec"]) / gp_time * 100
        return f"~{speedup:.0f}% faster than GeoPandas" if speedup > 0 else "Slower than GeoPandas"
    return "Fast performance"


def update_readme() -> bool:
    """Update README with benchmark results. Returns True if updated."""
    # Validate files exist and get results
    if not README_FILE.exists():
        print(f"README file not found: {README_FILE}")
        return False

    results = _get_results()
    if results is None:
        return False

    # Validate README structure and get content
    readme_content = README_FILE.read_text(encoding="utf-8")
    if not _validate_markers(readme_content):
        return False

    # Process the README update
    return _process_update(readme_content, results)


def _get_results() -> dict | None:
    """Get benchmark results from a file or create placeholder."""
    if not RESULTS_FILE.exists():
        print(f"Results file not found: {RESULTS_FILE}")
        return {"meta": {"skipped": True, "reason": "No benchmark results available yet"}, "runs": {}}

    try:
        return json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"Error reading results file: {e}")
        return None


def _validate_markers(content: str) -> bool:
    """Validate that both markers exist in README content."""
    if START_MARKER not in content:
        print(f"Start marker '{START_MARKER}' not found in README")
        return False
    if END_MARKER not in content:
        print(f"End marker '{END_MARKER}' not found in README")
        return False
    return True


def _process_update(readme_content: str, results: dict) -> bool:
    """Process the README update with new results."""
    # Find marker positions
    start_idx = readme_content.find(START_MARKER)
    end_idx = readme_content.find(END_MARKER, start_idx)

    if start_idx == -1 or end_idx == -1:
        print("Could not find both markers in README")
        return False

    # Extract before and after sections
    before = readme_content[: start_idx + len(START_MARKER)]
    after = readme_content[end_idx:]

    # Generate a new table
    table = create_results_table(results)

    # Reconstruct README
    new_content = f"{before}\n\n{table}\n\n{after}"

    # Write back if changed
    if new_content != readme_content:
        README_FILE.write_text(new_content, encoding="utf-8")
        print("README updated with latest benchmark results")
        return True
    print("README already up to date")
    return False


def main() -> int:
    """Update README with benchmark results."""
    try:
        update_readme()
    except (OSError, ValueError) as e:
        print(f"Error updating README: {e}")
        return 1
    else:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
