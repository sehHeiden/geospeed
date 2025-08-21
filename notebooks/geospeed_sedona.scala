import org.apache.sedona.spark.SedonaContext

val config = SedonaContext.builder()
.master("local[*]") // Delete this if run in cluster mode
.appName("readTestScala") // Change this to a proper name
.getOrCreate()

val sedona = SedonaContext.create(config)

// load the SHAPEFILES
val buildings_gdf = sedona.read.format("shapefile").option("recursiveFileLookup", "true").load("../ALKIS/*/GebauedeBauwerk.shp").dropDuplicates(["oid"])
buildings_gdf.createOrReplaceTempView("buildings")

val nutzung_gdf = sedona.read.format("shapefile").option("recursiveFileLookup", "true").load("../ALKIS/*/NutzungFlurstueck.shp").dropDuplicates(["oid"])
nutzung_gdf.createOrReplaceTempView("usage")

result_gdf = sedona.sql("""SELECT ST_Intersection(b.geometry, u.geometry) AS geometry, b.oid AS building_oid, b.gebnutzbez, b.gfkzshh, b.name, b.anzahlgs, b.gmdschl, b.lagebeztxt, b.funktion, u.oid AS flur_oid, u.nutzart, u.bez, u.flstkennz
                       FROM buildings b, usage u
                       WHERE ST_Intersects(b.geometry, u.geometry)"""
                   )

result_gdf.createOrReplaceTempView("result")

result2_gdf = sedona.sql("""
                        SELECT ST_MakeValid(geometry), building_oid, gebnutzbez, gfkzshh, name, anzahlgs, gmdschl, lagebeztxt, funktion, flur_oid, nutzart, bez, flstkennz
                        FROM result
                         """)