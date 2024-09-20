"""Test the execution speed for an equivalent of Geopandas Overlay with intersection using a spatial index."""

import time
from pathlib import Path

import duckdb

con = duckdb.connect()
con.install_extension("spatial")
con.load_extension("spatial")

print(
    f"Formats writable AND readable: {con.sql("SELECT short_name FROM ST_Drivers() WHERE can_create AND can_open;").fetchall()}"
)
start = time.time()

shapefile_dir = Path("../ALKIS")  # Base directory
building_files = shapefile_dir.glob("*/GebauedeBauwerk.shp")  # Glob pattern for the subdirectories
parcel_files = shapefile_dir.glob("*/NutzungFlurstueck.shp")  # Glob pattern for the subdirectories

# Check if table already exists, if not create it
table_created = False

# Iterate over the found shapefiles and load them into DuckDB
for building_file, parcel_file in zip(building_files, parcel_files, strict=True):
    if not table_created:
        # Create the table with the first shapefile
        con.sql(f"CREATE TABLE buildings AS SELECT * FROM ST_Read('{building_file.resolve()!s}');")
        con.sql(f"CREATE TABLE parcels AS SELECT * FROM ST_Read('{parcel_file.resolve()!s}');")

        table_created = True
    else:
        # Insert into the existing table for subsequent shapefiles
        con.sql(f"INSERT INTO buildings SELECT * FROM ST_Read('{building_file.resolve()!s}');")
        con.sql(f"INSERT INTO parcels SELECT * FROM ST_Read('{parcel_file.resolve()!s}');")

# Make the data valid
con.sql("""
    UPDATE buildings
    SET geom = ST_MakeValid(geom)
    WHERE NOT ST_IsValid(geom);

    UPDATE parcels
    SET geom = ST_MakeValid(geom)
    WHERE NOT ST_IsValid(geom);
""")

# create indexes
con.sql("CREATE INDEX buildings_idx ON buildings USING RTREE (geom);")
con.sql("CREATE INDEX parcels_idx ON parcels USING RTREE (geom);")
print(f"DuckDB: Loading data takes: {(time.time() - start):.2f} s.")

time_intersection = time.time()
con.sql("""
    CREATE TABLE buildings_intersection AS
    SELECT ST_Intersection(buildings.geom, parcels.geom) as geom,
    buildings.oid AS building_oid,
    parcels.oid AS parcel_oid
    FROM buildings, parcels
    WHERE ST_Intersects(buildings.geom, parcels.geom);
    """)

con.sql("""
    DROP INDEX buildings_idx;
    DROP INDEX parcels_idx;

    ALTER TABLE buildings
    DROP COLUMN geom;
    ALTER TABLE parcels
    DROP COLUMN geom;
    """)

con.sql("""
    CREATE TABLE intersections AS
    SELECT *
    FROM buildings_intersection AS bi, buildings AS bs, parcels AS ps
    WHERE bi.building_oid = bs.oid AND bi.parcel_oid = ps.oid;
    """)

print(f"DuckDB: Intersection takes: {(time.time() - time_intersection):.2f} s.")


time_writing = time.time()
con.sql("""
    COPY(SELECT * EXCLUDE geom, ST_AsWKB(geom) AS geometry
         FROM intersections
         WHERE ST_IsValid(geom) AND NOT ST_IsEmpty(geom))
    TO 'buildings_with_parcels.fgb' WITH(FORMAT GDAL, DRIVER 'FlatGeobuf')""")


print(f"DuckDB: Saving takes: {(time.time() - time_writing):.2f} s.")

print(f"Dask: Total duration: {(time.time() - start):.2f} s.")
