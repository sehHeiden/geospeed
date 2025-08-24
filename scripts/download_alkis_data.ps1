# Read configuration from file
$ConfigFile = Join-Path $PSScriptRoot "alkis_config.txt"
$Districts = @(Get-Content $ConfigFile | Where-Object { $_ -match '^DISTRICTS=' } | ForEach-Object { $_.Split('=')[1] })
$Layers = @(Get-Content $ConfigFile | Where-Object { $_ -match '^LAYERS=' } | ForEach-Object { $_.Split('=')[1] })
$BaseUrl = (Get-Content $ConfigFile | Where-Object { $_ -match '^BASE_URL=' } | ForEach-Object { $_.Split('=')[1] })

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.IO.Compression.FileSystem

# Calculate expected shapefile count
$ExpectedShp = $Districts.Count * $Layers.Count

# Check if data exists
if ((Test-Path "ALKIS_CI") -and ((Get-ChildItem -Path "ALKIS_CI" -Recurse -Filter "*.shp" -ErrorAction SilentlyContinue).Count -ge $ExpectedShp)) {
    Write-Host "[OK] Data exists" -ForegroundColor Green
    exit 0
}

Write-Host "[DOWNLOAD] Downloading ALKIS_CI data..." -ForegroundColor Yellow
New-Item -ItemType Directory -Path "ALKIS_CI" -Force | Out-Null

foreach ($district in $Districts) {
    New-Item -ItemType Directory -Path "ALKIS_CI/alkis_shape_$district" -Force | Out-Null
    
    # Download
    Write-Host "Downloading $district..." -ForegroundColor Gray
    Invoke-WebRequest -Uri "$BaseUrl/alkis_shape_$district.zip" -OutFile "$district.zip" -UseBasicParsing
    
    # Extract layers
    $zip = [System.IO.Compression.ZipFile]::OpenRead("$district.zip")
    foreach ($layer in $Layers) {
        $zip.Entries | Where-Object { $_.Name -match "^$layer\.(shp|shx|dbf|prj)$" } | ForEach-Object {
            [System.IO.Compression.ZipFileExtensions]::ExtractToFile($_, "ALKIS_CI/alkis_shape_$district/$($_.Name)", $true)
        }
    }
    $zip.Dispose()
    
    Remove-Item "$district.zip" -Force
}

$shpCount = (Get-ChildItem -Path "ALKIS_CI" -Recurse -Filter "*.shp").Count
Write-Host "[OK] Downloaded $shpCount shapefiles" -ForegroundColor Green
