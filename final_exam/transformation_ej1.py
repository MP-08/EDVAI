from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import DateType
from pyspark.sql.functions import when, col

spark = SparkSession.builder \
    .appName("MiAplicacionHive") \
    .enableHiveSupport() \
    .getOrCreate()

df_inf21 = (
    spark.read.option("header", "true")
    .option("delimiter", ";")
    .csv("hdfs://172.17.0.2:9000/ingest/2021-informe.csv")
)

df_inf22 = (
    spark.read.option("header", "true")
    .option("delimiter", ";")
    .csv("hdfs://172.17.0.2:9000/ingest/202206-informe.csv")
)

df_u = df_inf21.union(df_inf22)

df_union = df_u.withColumn(
    "Fecha", to_date(df_u["Fecha"], "dd/MM/yyyy").cast(DateType())
    )

df_union.createOrReplaceTempView("v_union")

df_info_union = spark.sql("""
    SELECT 
        CAST(Fecha AS DATE) AS fecha,
        CAST(`Hora UTC` AS STRING) AS horaUTC,
        CAST(`Clase de Vuelo (todos los vuelos)` AS STRING) AS clase_de_vuelo,
        CAST(`Clasificación Vuelo` AS STRING) AS clasificacion_de_vuelo,
        CAST(`Tipo de Movimiento` AS STRING) AS tipo_de_movimiento,
        CAST(Aeropuerto AS STRING) AS aeropuerto,
        CAST(`Origen / Destino` AS STRING) AS origen_destino,
        CAST(`Aerolinea Nombre` AS STRING) AS aerolinea_nombre,
        CAST(Aeronave AS STRING) AS aeronave,
        CAST(Pasajeros AS INTEGER) AS pasajeros
    FROM v_union
    WHERE `Clasificación Vuelo` <> 'Internacional'
""")

df_info_notnull = df_info_union.withColumn(
    "pasajeros", when(col("pasajeros").isNull(), 0).otherwise(col("pasajeros"))
)

df_info_notnull.createOrReplaceTempView("info_tabla_view")

spark.sql("INSERT INTO db_ej1.h_info_tab SELECT * FROM info_tabla_view")


df_aerop = (
    spark.read.option("header", "true")
    .option("delimiter", ";")
    .csv("hdfs://172.17.0.2:9000/ingest/aeropuertos.csv")
)

df_aerop.createOrReplaceTempView("v_aerop")

df_aerop_tabla = spark.sql("""
    SELECT  
        CAST(local AS STRING) AS aeropuerto,
        CAST(oaci AS STRING) AS oaci,
        CAST(iata AS STRING) AS iata,
        CAST(tipo AS STRING) AS tipo,
        CAST(denominacion AS STRING) AS denominacion,
        CAST(coordenadas AS STRING) AS coordenadas,
        CAST(longitud AS STRING) AS longitud,
        CAST(latitud AS STRING) AS latitud,
        CAST(elev AS FLOAT) AS elev,
        CAST(uom_elev AS STRING) AS uom_elev,
        CAST(ref AS INTEGER) AS ref,
        CAST(distancia_ref AS FLOAT) AS distancia_ref,
        CAST(direccion_ref AS STRING) AS direccion_ref,
        CAST(condicion AS STRING) AS condicion,
        CAST(control AS STRING) AS control,
        CAST(region AS STRING) AS region,
        CAST(uso AS STRING) AS uso,
        CAST(trafico AS STRING) AS trafico,
        CAST(sna AS STRING) AS sna,
        CAST(concesionado AS STRING) AS concesionado,
        CAST(provincia AS STRING) AS provincia
    FROM v_aerop
""")

df_aerop_filtrado = df_aerop_tabla.withColumn(
    "distancia_ref", when(col("distancia_ref").isNull(), 0).otherwise(col("distancia_ref"))
)

df_aerop_filtrado.createOrReplaceTempView("aero_view")

spark.sql("INSERT INTO db_ej1.h_aerop_tabla SELECT * FROM aero_view")