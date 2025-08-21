# CI Dataset Information

## Counties Included
- **Brandenburg City (brb)**: Central Brandenburg, kreisfreie Stadt  
- **Potsdam (p)**: State capital, neighboring region

## Files per County  
- GebauedeBauwerk.shp
- GebauedeBauwerk.shx
- GebauedeBauwerk.dbf
- GebauedeBauwerk.prj
- NutzungFlurstueck.shp
- NutzungFlurstueck.shx
- NutzungFlurstueck.dbf
- NutzungFlurstueck.prj

## Size Analysis
- Total dataset: 288.8MB
- Estimated peak memory: ~2.8GB
- GitHub repo limit: ✅ Under 1GB
- CI memory limit: ✅ Under 7GB

## Purpose
This minimal dataset enables CI/CD testing with:
- Realistic GIS intersection operations
- Multiple county border handling (duplicate OIDs)
- All framework compatibility testing
- Manageable memory footprint for GitHub Actions

## Geographic Coverage
Central Brandenburg region with urban areas (cities) providing
good test coverage for building-parcel intersection scenarios.
