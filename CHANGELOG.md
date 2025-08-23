# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Fixed geofileops benchmark compatibility with modern geofileops API versions
- Removed deprecated `copy_layer()` method calls that were causing AttributeError
- Added robust version detection for geofileops with fallback to `overlay()` method
- Improved error handling with clearer messages for unsupported geofileops versions

### Added
- Comprehensive tests for geofileops API compatibility
- Helper function `_do_intersection()` for version-tolerant spatial operations

### Changed
- **BREAKING**: geofileops benchmark now requires `geofileops>=0.8.0` for proper API support
- Refactored geofileops integration to use `intersection()` or `overlay()` methods instead of deprecated `copy_layer()`
