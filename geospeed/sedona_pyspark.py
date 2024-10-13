"""Execute the pyspark code tested in a jupyter notebook as python file."""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sedona.spark import SedonaContext
from sedona.sql import ST_GeoHash, ST_Intersection, ST_Intersects, ST_MakeValid

start_time = time.time()


# Initialize SparkSession with master configuration
spark = (
    SparkSession.builder.appName("SedonaApp")
    .master("spark://172.18.0.2:7077")
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.6.1,org.datasyslab:geotools-wrapper:1.6.1-28.2",
    )
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "12g")
    .getOrCreate()
)

# Create Sedona Context
sedona = SedonaContext.create(spark)

building_columns = ["oid", "gebnutzbez", "gfkzshh", "name", "anzahlgs", "gmdschl", "lagebeztxt", "funktion"]
usage_columns = ["oid", "nutzart", "bez", "flstkennz"]

build_gdf = (
    sedona.read.format("shapefile")
    .option("recursiveFileLookup", "true")
    .load("../ALKIS/*/GebauedeBauwerk.shp")
    .dropDuplicates(["oid"])
)
build_gdf = build_gdf.select(
    ST_MakeValid(build_gdf["geometry"]).alias("geometry"), *[build_gdf[col] for col in building_columns]
)
build_gdf.createOrReplaceTempView("buildings")
use_gdf = (
    sedona.read.format("shapefile")
    .option("recursiveFileLookup", "true")
    .load("../ALKIS/*/NutzungFlurstueck.shp")
    .dropDuplicates(["oid"])
)
use_gdf = use_gdf.select(ST_MakeValid(use_gdf["geometry"]).alias("geometry"), *[use_gdf[col] for col in usage_columns])
use_gdf.createOrReplaceTempView("usage")

unchanged_building_columns = [col(f"b.{name}") for name in set(building_columns).difference(["oid"])]
unchanged_usage_columns = [col(f"b.{name}") for name in set(usage_columns).difference(["oid"])]

result_gdf = (
    build_gdf.alias("b")
    .join(use_gdf.alias("u"), ST_Intersects(col("b.geometry"), col("u.geometry")))
    .select(
        ST_Intersection(col("b.geometry"), col("u.geometry")).alias("geometry"),
        col("b.oid").alias("building_oid"),
        *unchanged_building_columns,
        col("u.oid").alias("flur_oid"),
        *unchanged_usage_columns,
    )
)

proj = """{"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json","type": "ProjectedCRS","name": "ETRS89 / UTM zone 33N","base_crs": {"name": "ETRS89","datum_ensemble": {"name": "European Terrestrial Reference System 1989 ensemble","members": [{"name": "European Terrestrial Reference Frame 1989"},{"name": "European Terrestrial Reference Frame 1990"},{"name": "European Terrestrial Reference Frame 1991"},{"name": "European Terrestrial Reference Frame 1992"},{"name": "European Terrestrial Reference Frame 1993"},{"name": "European Terrestrial Reference Frame 1994"},{"name": "European Terrestrial Reference Frame 1996"},{"name": "European Terrestrial Reference Frame 1997"},{"name": "European Terrestrial Reference Frame 2000"},{"name": "European Terrestrial Reference Frame 2005"},{"name": "European Terrestrial Reference Frame 2014"}],"ellipsoid": {"name": "GRS 1980","semi_major_axis": 6378137,"inverse_flattening": 298.257222101},"accuracy": "0.1"},"coordinate_system": {"subtype": "ellipsoidal","axis": [{"name": "Geodetic latitude","abbreviation": "Lat","direction": "north","unit": "degree"},{"name": "Geodetic longitude","abbreviation": "Lon","direction": "east","unit": "degree"}]},"id": {"authority": "EPSG","code": 4258}},"conversion": {"name": "UTM zone 33N","method": {"name": "Transverse Mercator","id": {"authority": "EPSG","code": 9807}},"parameters": [{"name": "Latitude of natural origin","value": 0,"unit": "degree","id": {"authority": "EPSG","code": 8801}},{"name": "Longitude of natural origin","value": 15,"unit": "degree","id": {"authority": "EPSG","code": 8802}},{"name": "Scale factor at natural origin","value": 0.9996,"unit": "unity","id": {"authority": "EPSG","code": 8805}},{"name": "False easting","value": 500000,"unit": "metre","id": {"authority": "EPSG","code": 8806}},{"name": "False northing","value": 0,"unit": "metre","id": {"authority": "EPSG","code": 8807}}]},"coordinate_system": {"subtype": "Cartesian","axis": [{"name": "Easting","abbreviation": "E","direction": "east","unit": "metre"},{"name": "Northing","abbreviation": "N","direction": "north","unit": "metre"}]},"scope": "Engineering survey, topographic mapping.","area": "Europe between 12°E and 18°E: Austria; Denmark - offshore and offshore; Germany - onshore and offshore; Norway including Svalbard - onshore and offshore.","bbox": {"south_latitude": 46.4,"west_longitude": 12,"north_latitude": 84.42,"east_longitude": 18.01},"id": {"authority": "EPSG","code": 25833}}"""
result_columns = [
    "building_oid",
    "gebnutzbez",
    "gfkzshh",
    "name",
    "anzahlgs",
    "gmdschl",
    "lagebeztxt",
    "funktion",
    "flur_oid",
    "nutzart",
    "bez",
    "flstkennz",
]


(
    result_gdf.select(
        result_gdf["geometry"],
        ST_GeoHash(result_gdf["geometry"], 5).alias("geom_hash"),
        *[result_gdf[col] for col in result_columns],
    )
    .orderBy("geom_hash")
    .coalesce(1)
    .write.format("geoparquet")
    .mode("overwrite")
    .option("geoparquet.version", "1.0.0")
    .option("geoparquet.crs", proj)
    .option("parquet.compression", "zstd")
    .save("sedona_data.parquet")
)
spark.stop()
print(f"Excecution took: {time.time() - start_time} sec.")
