#!/usr/bin/env python
"""Execute PySpark/Sedona benchmark in standalone mode for CI environments."""

import os
import sys
import time
from pathlib import Path

# Add timing
start_time = time.time()

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from sedona.spark import SedonaContext
    from sedona.sql import ST_GeoHash, ST_Intersection, ST_Intersects, ST_MakeValid
except ImportError as e:
    print(f"PySpark/Sedona imports failed: {e}")
    print("This requires PySpark and Apache Sedona to be installed.")
    sys.exit(1)

# Ensure JAVA_HOME is set (common issue in CI)
# Check if already set first
if os.environ.get("JAVA_HOME"):
    print(f"JAVA_HOME already set to: {os.environ['JAVA_HOME']}")
else:
    # Try common Java locations
    java_paths = [
        "/usr/lib/jvm/java-11-openjdk-amd64",  # Ubuntu
        "/usr/lib/jvm/java-17-openjdk-amd64",  # Ubuntu
        "/usr/lib/jvm/default-java",  # Ubuntu
        "/opt/java/openjdk",  # Docker
    ]
    for java_path in java_paths:
        if Path(java_path).exists():
            os.environ["JAVA_HOME"] = java_path
            print(f"Set JAVA_HOME to: {java_path}")
            break
    else:
        # Try to find Java with which command
        import subprocess
        try:
            java_cmd = subprocess.run(["which", "java"], capture_output=True, text=True, check=True)
            java_bin = java_cmd.stdout.strip()
            # Get the real path in case it's a symlink
            java_real = subprocess.run(["readlink", "-f", java_bin], capture_output=True, text=True, check=True)
            java_home = str(Path(java_real.stdout.strip()).parent.parent)
            os.environ["JAVA_HOME"] = java_home
            print(f"Found Java via which command, set JAVA_HOME to: {java_home}")
        except subprocess.CalledProcessError:
            print("JAVA_HOME not set and no Java installation found")
            sys.exit(1)

# Initialize SparkSession in local mode for CI
spark = (
    SparkSession.builder.appName("SedonaCI")
    .master("local[*]")  # Use all available cores locally
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.6.1,org.datasyslab:geotools-wrapper:1.6.1-28.2",
    )
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    .config("spark.driver.memory", "4g")  # Reduced for CI
    .config("spark.executor.memory", "4g")  # Reduced for CI
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

# Suppress Spark logging
spark.sparkContext.setLogLevel("WARN")

try:
    # Create Sedona Context
    sedona = SedonaContext.create(spark)

    building_columns = ["oid", "gebnutzbez", "gfkzshh", "name", "anzahlgs", "gmdschl", "lagebeztxt", "funktion"]
    usage_columns = ["oid", "nutzart", "bez", "flstkennz"]

    # Import utils for data directory detection
    import sys
    from pathlib import Path
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))
    from geospeed.utils import get_data_dir
    
    # Check for data directory
    try:
        data_dir = get_data_dir()
        print(f"Using data directory: {data_dir}")
    except FileNotFoundError:
        print("No ALKIS data found - skipping Sedona benchmark")
        sys.exit(0)

    print("Loading buildings data...")
    build_gdf = (
        sedona.read.format("shapefile")
        .option("recursiveFileLookup", "true")
        .load(f"{data_dir}/*/GebauedeBauwerk.shp")
        .dropDuplicates(["oid"])
    )
    build_gdf = build_gdf.select(
        ST_MakeValid(build_gdf["geometry"]).alias("geometry"), *[build_gdf[col_name] for col_name in building_columns]
    )
    build_gdf.createOrReplaceTempView("buildings")

    print("Loading parcels data...")
    use_gdf = (
        sedona.read.format("shapefile")
        .option("recursiveFileLookup", "true")
        .load(f"{data_dir}/*/NutzungFlurstueck.shp")
        .dropDuplicates(["oid"])
    )
    use_gdf = use_gdf.select(
        ST_MakeValid(use_gdf["geometry"]).alias("geometry"), *[use_gdf[col_name] for col_name in usage_columns]
    )
    use_gdf.createOrReplaceTempView("usage")

    print("Performing intersection...")
    unchanged_building_columns = [col(f"b.{name}") for name in set(building_columns).difference(["oid"])]
    unchanged_usage_columns = [col(f"u.{name}") for name in set(usage_columns).difference(["oid"])]

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

    # Save results
    print("Saving results...")
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
            *[result_gdf[col_name] for col_name in result_columns],
        )
        .orderBy("geom_hash")
        .coalesce(1)
        .write.format("geoparquet")
        .mode("overwrite")
        .option("geoparquet.version", "1.0.0")
        .option("parquet.compression", "zstd")
        .save("sedona_data.parquet")
    )

    execution_time = time.time() - start_time
    print(f"Sedona PySpark execution took: {execution_time:.1f} seconds")

except RuntimeError as e:
    print(f"Sedona execution failed: {e}")
    sys.exit(1)
finally:
    spark.stop()
