"""Tests for geofileops benchmark functionality."""
import sys
import unittest.mock
from pathlib import Path
from unittest import TestCase

# Add the parent directory to sys.path to import geospeed modules
sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock geofileops import to avoid ImportError during testing
with unittest.mock.patch.dict('sys.modules', {'geofileops': unittest.mock.MagicMock()}):
    from geospeed.geofileops import _do_intersection, _raise_geofileops_methods_error


class TestGeofileOpsBenchmark(TestCase):
    """Test geofileops benchmark API compatibility."""

    def test_intersection_with_modern_api(self) -> None:
        """Test _do_intersection with modern API (has intersection method)."""
        # Mock geofileops API with intersection method
        mock_gfo = unittest.mock.Mock()
        mock_gfo.intersection = unittest.mock.Mock()

        _do_intersection(
            mock_gfo,
            "input1.gpkg",
            "input2.gpkg", 
            "output.gpkg",
            input1_columns=["col1"],
            input2_columns=["col2"]
        )

        mock_gfo.intersection.assert_called_once_with(
            "input1.gpkg",
            "input2.gpkg",
            "output.gpkg",
            input1_columns=["col1"],
            input2_columns=["col2"]
        )

    def test_overlay_with_legacy_api(self) -> None:
        """Test _do_intersection with legacy API (has overlay but not intersection)."""
        # Mock geofileops API with only overlay method
        mock_gfo = unittest.mock.Mock()
        mock_gfo.overlay = unittest.mock.Mock()
        # Explicitly remove intersection to simulate old API
        del mock_gfo.intersection

        _do_intersection(
            mock_gfo,
            "input1.gpkg", 
            "input2.gpkg",
            "output.gpkg",
            input1_columns=["col1"],
            input2_columns=["col2"]
        )

        mock_gfo.overlay.assert_called_once_with(
            input1="input1.gpkg",
            input2="input2.gpkg", 
            out="output.gpkg",
            operation="intersection",
            input1_columns=["col1"],
            input2_columns=["col2"]
        )

    def test_unsupported_api_raises_error(self) -> None:
        """Test that unsupported API raises AttributeError with helpful message."""
        # Mock geofileops API with neither intersection nor overlay
        mock_gfo = unittest.mock.Mock()
        del mock_gfo.intersection
        del mock_gfo.overlay

        with self.assertRaisesRegex(AttributeError, "Neither 'intersection' nor 'overlay' method available"):
            _do_intersection(
                mock_gfo,
                "input1.gpkg",
                "input2.gpkg", 
                "output.gpkg"
            )

    def test_raise_geofileops_methods_error(self) -> None:
        """Test that helper function properly raises AttributeError."""
        with self.assertRaisesRegex(AttributeError, "Test error message"):
            _raise_geofileops_methods_error("Test error message")


if __name__ == "__main__":
    import unittest
    unittest.main()
