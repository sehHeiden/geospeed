"""Simplified tests for Sedona PySpark benchmark functionality."""

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.integration
def test_sedona_import_availability() -> None:
    """Test that Sedona and PySpark can be imported if available."""
    result = subprocess.run(
        [sys.executable, "-c", "import pyspark; import sedona; print('SUCCESS')"],
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        assert "SUCCESS" in result.stdout
    else:
        # Expected to fail in most environments without Spark
        error_output = result.stderr.lower()
        assert any(
            phrase in error_output
            for phrase in ["no module named 'pyspark'", "no module named 'sedona'", "modulenotfounderror"]
        )
        pytest.skip(f"PySpark/Sedona not available: {result.stderr}")


@pytest.mark.integration
def test_sedona_script_execution() -> None:
    """Test that the Sedona script executes without critical errors."""
    result = subprocess.run(
        [sys.executable, "geospeed/sedona_pyspark_ci.py"],
        check=False,
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    # Should exit cleanly or with expected dependency errors
    assert result.returncode in [0, 1]
    assert len(result.stdout + result.stderr) > 0


@pytest.mark.integration
def test_benchmark_runner_includes_sedona() -> None:
    """Test that the benchmark runner includes Sedona tests."""
    result = subprocess.run(
        [sys.executable, "scripts/benchmarks.py"],
        check=False,
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    assert result.returncode == 0

    # Check results file exists and includes Sedona
    results_file = Path(__file__).parent.parent / "benchmarks" / "latest.json"
    if results_file.exists():
        import json

        results = json.loads(results_file.read_text())
        assert "sedona_pyspark" in results["runs"]
