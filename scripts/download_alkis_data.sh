#!/bin/bash
set -euo pipefail

# Read configuration from file
CONFIG_FILE="$(dirname "$0")/alkis_config.txt"

# Check config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: Config file not found: $CONFIG_FILE"
    exit 1
fi

# Read configuration with proper parsing
DISTRICTS=$(grep '^DISTRICTS=' "$CONFIG_FILE" | cut -d= -f2 | tr '\n' ' ' | xargs)
LAYERS=$(grep '^LAYERS=' "$CONFIG_FILE" | cut -d= -f2 | tr '\n' ' ' | xargs)
BASE_URL=$(grep '^BASE_URL=' "$CONFIG_FILE" | cut -d= -f2 | tr -d '\r\n')

# Validate configuration
if [[ -z "$DISTRICTS" || -z "$LAYERS" || -z "$BASE_URL" ]]; then
    echo "Error: Missing configuration in $CONFIG_FILE"
    echo "DISTRICTS: '$DISTRICTS'"
    echo "LAYERS: '$LAYERS'"
    echo "BASE_URL: '$BASE_URL'"
    exit 1
fi

echo "Debug: DISTRICTS='$DISTRICTS'"
echo "Debug: LAYERS='$LAYERS'"
echo "Debug: BASE_URL='$BASE_URL'"

# Calculate expected shapefile count
district_count=$(echo $DISTRICTS | wc -w)
layer_count=$(echo $LAYERS | wc -w)
expected_shp=$((district_count * layer_count))

# Check if data exists
[[ -d ALKIS_CI && $(find ALKIS_CI -name "*.shp" | wc -l) -ge $expected_shp ]] && { echo "✅ Data exists"; exit 0; }

echo "📥 Downloading ALKIS_CI data..."
mkdir -p ALKIS_CI

for district in $DISTRICTS; do
    mkdir -p "ALKIS_CI/alkis_shape_$district"
    
    # Download
    echo "Downloading $district..."
    curl -L -o "$district.zip" "$BASE_URL/alkis_shape_$district.zip"
    
    # Extract layers
    for layer in $LAYERS; do
        unzip -j "$district.zip" "$layer.*" -d "ALKIS_CI/alkis_shape_$district/" >/dev/null 2>&1
    done
    
    rm "$district.zip"
done

echo "✅ Downloaded $(find ALKIS_CI -name '*.shp' | wc -l) shapefiles"
