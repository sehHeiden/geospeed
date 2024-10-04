"""Test the execution speed for an equivalent of Geopandas Overlay with intersection using a spatial index."""

import time
from pathlib import Path

import duckdb

save_name = "geo_speed.duckdb"
con = duckdb.connect("geo_speed.duckdb", config={"threads": 15, "memory_limit": "20GB"})
con.install_extension("spatial")
con.load_extension("spatial")

con.sql("DROP TABLE IF EXISTS buildings")
con.sql("DROP TABLE IF EXISTS parcels")
con.sql("DROP TABLE IF EXISTS buildings_intersection")
con.sql("DROP VIEW IF EXISTS intersections")

# Check supported formats for writing
formats = con.sql("SELECT short_name FROM ST_Drivers() WHERE can_create;").fetchall()
print(f"Writable formats: {formats}")
start = time.time()

shapefile_dir = Path("./ALKIS")  # Base directory
building_files = list(shapefile_dir.glob("*/GebauedeBauwerk.shp"))  # Glob pattern for the subdirectories
parcel_files = list(shapefile_dir.glob("*/NutzungFlurstueck.shp"))  # Glob pattern for the subdirectories

# Error handling if no files are found
if not building_files or not parcel_files:
    load_error_txt = "No shapefiles found in the provided directory."
    raise FileNotFoundError(load_error_txt)

# Create then insert
con.sql(f"CREATE TABLE buildings AS SELECT * FROM ST_Read('{building_files[0].resolve()!s}');")
con.sql(f"CREATE TABLE parcels AS SELECT * FROM ST_Read('{parcel_files[0].resolve()!s}');")
con.execute(
    """PREPARE insert_buildings_stmt AS
       INSERT INTO buildings SELECT *
       FROM ST_Read($1)
       WHERE oid NOT IN (SELECT oid FROM parcels);"""
)
con.execute(
    """PREPARE insert_parcels_stmt AS
       INSERT INTO parcels SELECT *
       FROM ST_Read($1)
       WHERE oid NOT IN(SELECT oid FROM parcels);"""
)


# Iterate over the found shapefiles and load them into DuckDB
for building_file, parcel_file in zip(building_files[1:], parcel_files[1:], strict=True):
    # Insert into the existing table for subsequent shapefiles
    con.execute(f"EXECUTE insert_buildings_stmt('{building_file.resolve()!s}')")
    con.execute(f"EXECUTE insert_parcels_stmt('{parcel_file.resolve()!s}')")
# Make the data valid
# Make geometries valid
con.sql("""
    UPDATE buildings SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom);
    UPDATE parcels SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom);
""")


# create indexes
con.sql("CREATE INDEX buildings_idx ON buildings USING RTREE (geom);")
con.sql("CREATE INDEX parcels_idx ON parcels USING RTREE (geom);")
print(f"DuckDB: Loading data takes: {(time.time() - start):.0f} s.")

# Perform intersection
time_intersection = time.time()
con.sql("""
    CREATE TABLE buildings_intersection AS
    SELECT ST_Intersection(buildings.geom, parcels.geom) as geom,
    buildings.oid AS building_oid,
    parcels.oid AS parcel_oid
    FROM buildings, parcels
    WHERE ST_Intersects(buildings.geom, parcels.geom);
    """)

# Drop the indexes and unnecessary columns
con.sql("""
    DROP INDEX buildings_idx;
    DROP INDEX parcels_idx;
    ALTER TABLE buildings DROP COLUMN geom;
    ALTER TABLE parcels DROP COLUMN geom;
""")

# Create final intersections table
con.sql("""
    CREATE VIEW intersections AS
    SELECT *
    FROM buildings_intersection AS bi
    JOIN buildings AS bs ON bi.building_oid = bs.oid
    JOIN parcels AS ps ON bi.parcel_oid = ps.oid;
    """)
con.sql("""UPDATE buildings_intersection SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom);""")
print(f"DuckDB: Intersection takes: {(time.time() - time_intersection):.0f} s.")

if not save_name:
    # Save the result to a file
    time_writing = time.time()
    con.sql("""
        COPY(SELECT * EXCLUDE geom, ST_AsWKB(geom) AS geometry
             FROM intersections
             WHERE ST_IsValid(geom) AND NOT ST_IsEmpty(geom))
        TO 'buildings_with_parcels.fgb' WITH(FORMAT GDAL, DRIVER 'FlatGeobuf', SRS 'EPSG:25833')""")
    print(f"DuckDB: Saving takes: {(time.time() - time_writing):.0f} s.")

print(f"DuckDB: Total duration: {(time.time() - start):.0f} s.")
