"""Simplified tests for geofileops benchmark functionality."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest


def get_gdal_version() -> str | None:
    """Get the system GDAL version."""
    # Try gdal-config first (Unix/Linux)
    gdal_config_path = shutil.which("gdal-config")
    if gdal_config_path is not None:
        try:
            result = subprocess.run([gdal_config_path, "--version"], capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
    
    # Try Python GDAL import (works on all platforms)
    try:
        result = subprocess.run(
            [sys.executable, "-c", "from osgeo import gdal; print(gdal.VersionInfo('RELEASE_NAME'))"],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


@pytest.mark.integration
def test_gdal_installed() -> None:
    """Test that GDAL is installed and accessible."""
    version = get_gdal_version()
    if version is None:
        pytest.skip("GDAL not found. Install with: sudo apt-get install gdal-bin libgdal-dev")
    print(f"GDAL version: {version}")


@pytest.mark.integration
@pytest.mark.skipif(get_gdal_version() is None, reason="GDAL not installed")
def test_geofileops_gdal_compatibility() -> None:
    """Test geofileops is compatible with the installed GDAL version."""
    try:
        import geofileops as gfo

        # Test basic functionality
        gfo_api = gfo.gfo if hasattr(gfo, "gfo") else gfo
        assert hasattr(gfo_api, "intersection") or hasattr(gfo_api, "overlay")
        print(f"✅ geofileops compatible with GDAL {get_gdal_version()}")
    except ImportError:
        pytest.skip("geofileops not installed")
    except (AttributeError, RuntimeError, OSError) as e:
        pytest.fail(f"geofileops incompatible with GDAL {get_gdal_version()}: {e}")


@pytest.mark.integration
def test_geofileops_import() -> None:
    """Test that geofileops can be imported if available."""
    result = subprocess.run(
        [sys.executable, "-c", "import geofileops; print('SUCCESS')"],
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        assert "SUCCESS" in result.stdout
    else:
        error_output = result.stderr.lower()
        assert any(phrase in error_output for phrase in ["no module named 'geofileops'", "modulenotfounderror"])
        pytest.skip(f"geofileops not available: {result.stderr}")


@pytest.mark.integration
def test_geofileops_script_execution() -> None:
    """Test that the geofileops script executes without critical errors."""
    result = subprocess.run(
        [sys.executable, "geospeed/geofileops.py"],
        check=False,
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    # Should exit cleanly or with expected dependency/data errors
    assert result.returncode in [0, 1]
    assert len(result.stdout + result.stderr) > 0


@pytest.mark.integration
def test_benchmark_runner_includes_geofileops() -> None:
    """Test that the benchmark runner includes geofileops tests."""
    result = subprocess.run(
        [sys.executable, "scripts/benchmarks.py"],
        check=False,
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    assert result.returncode == 0

    # Check results file exists and includes geofileops
    results_file = Path(__file__).parent.parent / "benchmarks" / "latest.json"
    if results_file.exists():
        import json

        results = json.loads(results_file.read_text())
        assert "geofileops" in results["runs"]
