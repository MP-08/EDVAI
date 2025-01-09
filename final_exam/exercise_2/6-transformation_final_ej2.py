from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import DateType
from pyspark.sql.functions import when, col
from pyspark.sql.functions import lower

spark = SparkSession.builder \
    .appName("MiAplicacionHive") \
    .enableHiveSupport() \
    .getOrCreate()

df_rental = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .csv("hdfs://172.17.0.2:9000/ingest/car_rental.csv")
)

df_rental.createOrReplaceTempView("v_rental")

df_rental_mod = spark.sql("""
    SELECT 
        CAST(fuelType AS STRING) AS fuelType,
        CAST(rating AS INTEGER) AS rating,
        CAST(renterTripsTaken AS INTEGER) AS renterTripsTaken,
        CAST(reviewCount AS INTEGER) AS reviewCount,
        CAST(`location.city` AS STRING) AS city,
        CAST(`location.state` AS STRING) AS state_name,
        CAST(`owner.id` AS INTEGER) AS owner_id,
        CAST(`rate.daily` AS INTEGER) AS rate_daily,
        CAST(`vehicle.make` AS STRING) AS make,
        CAST(`vehicle.model` AS STRING) AS model,
        CAST(`vehicle.year` AS INTEGER) AS year
    FROM v_rental
""")

df_rental_mod_clean = df_rental_mod.filter(df_rental_mod["rating"].isNotNull())


df_rental_mod = df_rental_mod.withColumn("fuelType", lower(df_rental_mod["fuelType"]))

df_georef = (
        spark.read.option("header", "true")
        .option("delimiter", ";")
        .csv("hdfs://172.17.0.2:9000/ingest/georef_usa.csv")
)

df_georef.createOrReplaceTempView("v_georef")

df_georef_mod = spark.sql("""
    SELECT
        CAST(`Geo Point` AS STRING) AS geo_point,
        CAST(`Geo Shape` AS STRING) AS geo_shape,
        CAST(`Official Code State` AS STRING) AS official_code_state,
        CAST(`Official Name State` AS STRING) AS official_name_state,
        CAST(`Iso 3166-3 Area Code` AS STRING) AS iso_3166_3_area_code,
        CAST(`Type` AS STRING) AS type,
        CAST(`United States Postal Service state abbreviation` AS STRING) AS usps_state_abbreviation,
        CAST(`State FIPS Code` AS STRING) AS state_fips_code,
        CAST(`State GNIS Code` AS STRING) AS state_gnis_code
    FROM v_georef
""")

df_merged = df_rental_mod.join(df_georef_mod, df_rental_mod.state_name == df_georef_mod.usps_state_abbreviation, "inner")

df_merged_filtered = df_merged.filter(df_merged.state_name != "TX")

df_merged_filtered.createOrReplaceTempView("view_car_rental")

spark.sql("""
INSERT INTO TABLE car_rental_db.car_rental_analytics
SELECT * FROM view_car_rental
""")