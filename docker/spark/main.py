from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import expr, element_at, split, input_file_name, \
    countDistinct, col, udf, max, min, concat_ws, current_timestamp, to_timestamp, collect_list, unix_timestamp, round
import os
from enum import Enum
import json
from geopy import distance

scala_version = '2.12'
spark_version = '3.2.2'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.2'
]
kafka_route = os.environ.get('kafkaRoute')

class ColumnName(Enum):
    COD_LINEA = "codLinea"
    LON = "lon"
    LAT = "lat"
    COD_PARADA = "codParada"
    ORDEN = "orden"

def obtener_paradas(values, spark, filter_4):
    df = spark \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("lineasyparadas.csv")

    parada1 = filter_4['paradaInicio']
    parada2 = filter_4['paradaFin']
    lista = [parada1, parada2]

    # Obtener las lineas que pasan por las dos paradas
    codLineaLista = df \
        .select(ColumnName.COD_PARADA.value, ColumnName.COD_LINEA.value, ColumnName.LON.value, ColumnName.LAT.value) \
        .filter(df.codParada.isin(lista)) \
        .groupby(ColumnName.COD_LINEA.value).agg((countDistinct(ColumnName.COD_PARADA.value)).alias("numero_paradas")) \
        .filter(col("numero_paradas") == 2) \
        .select('codLinea').collect()

    array_lineas = [str(row.codLinea) for row in codLineaLista]

    # Obtener el orden maximo y minimo de cada linea
    dfOrdenLineas = df \
        .select(ColumnName.COD_PARADA.value, ColumnName.COD_LINEA.value, ColumnName.ORDEN.value) \
        .filter(df.codLinea.isin(array_lineas)) \
        .groupby(ColumnName.COD_LINEA.value) \
        .agg(min(ColumnName.ORDEN.value).alias("min"), max(ColumnName.ORDEN.value).alias("max")) \
        .collect()

    dict_orden_lineas = {}
    for row in dfOrdenLineas:
        dict_orden_lineas[int(row.codLinea)] = {'max': row.max, 'min': row.min}

    # Obtener, en cada linea, el orden de la parada
    dfOrdenParadaInicial = df \
        .select(ColumnName.COD_PARADA.value, ColumnName.COD_LINEA.value, ColumnName.ORDEN.value) \
        .filter(df.codLinea.isin(array_lineas)) \
        .filter(df.codParada == parada1) \
        .collect()

    dict_orden_objetivo = {}
    for row in dfOrdenParadaInicial:
        dict_orden_objetivo[int(row.codLinea)] = row.orden

    dfOrdenParadas = df.select(
        concat_ws('_', ColumnName.COD_PARADA.value, ColumnName.COD_LINEA.value).alias('parada_linea'),
        ColumnName.ORDEN.value).collect()

    dict_orden_paradas = {}
    for row in dfOrdenParadas:
        dict_orden_paradas[row.parada_linea] = row.orden

    def get_distancia_parada(codParIni, codLinea):
        orden = int(dict_orden_paradas[codParIni + "_" + codLinea])
        codParIni = int(float(codParIni))
        codLinea = int(float(codLinea))
        paradaObjetivo = int(dict_orden_objetivo[codLinea])

        if orden >= paradaObjetivo:
            # +1 porque tiene que ir de la ultima a la primera
            return int(dict_orden_lineas[codLinea]['max']) - orden + paradaObjetivo - int(
                dict_orden_lineas[codLinea]['min']) + 1
        else:
            return int(paradaObjetivo - orden)

    get_distancia_paradas_cols = udf(get_distancia_parada, IntegerType())

    return values.filter(values.codLinea.isin(array_lineas)) \
        .withColumn("distanciaParadas", get_distancia_paradas_cols('codParIni', 'codLinea'))

def filtro_radio(values, filter_3):
    # Radio query
    def get_distancia(lat, lon):
        return distance.distance((float(lat), float(lon)), (filter_3['latitude'], filter_3['longitude'])).km * 1000

    get_distancia_cols = udf(get_distancia, FloatType())
    return values.withColumn("distancia", get_distancia_cols('lat', 'lon')).filter(col("distancia") < filter_3["radio"])


def filtro_minutos(values, filter_2):
    # Filtering Query    
    query_aux = values
    
    if("codLinea" in filter_2.keys()):
        query_aux = query_aux.filter(values["codLinea"] == filter_2["codLinea"])

    if("sentido" in filter_2.keys()):
        query_aux = query_aux.filter(values["sentido"] == filter_2["sentido"])

    if("last_update" in filter_2.keys()): 
        df_minutes = query_aux.withColumn('from_timestamp', to_timestamp(col('last_update'), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn('end_timestamp', current_timestamp()) \
        .withColumn('DiffInSeconds', unix_timestamp("end_timestamp") - unix_timestamp('from_timestamp')) \
        .withColumn('DiffInMinutes', round(col('DiffInSeconds') / 60))

        query_aux = df_minutes.filter(col('DiffInMinutes') <= int(filter_2["last_update"]))\
            .drop('end_timestamp').drop('end_timestamp').drop('DiffInSeconds').drop('DiffInMinutes')
        
    return query_aux.groupby(values.columns).agg(collect_list('codBus').alias('dummy')).drop('dummy')

def main(directory) -> None:
    with open("config.json", 'r') as f:
        filters = json.load(f)

    spark = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("lineasyubicaciones") \
        .config("spark.jars.packages", ",".join(packages)) \
        .getOrCreate()

    fields = [
        StructField("codBus", StringType(), True),
        StructField("codLinea", StringType(), True),
        StructField("sentido", StringType(), True),
        StructField("lon", StringType(), True),
        StructField("lat", StringType(), True),
        StructField("codParIni", StringType(), True),
        StructField("last_update", StringType(), True)
    ]

    lines = spark \
        .readStream \
        .format("csv") \
        .options(header='true') \
        .schema(StructType(fields)) \
        .load(directory) \
        .withColumn("filename", element_at(split(input_file_name(), "/"), -1)) \
        .withColumn("timestamp", element_at(split("filename", ".txt"), 1))

    values = lines
    
    query1 = values \
        .withColumn("id", expr("uuid()")) \
        .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark/checkpoint") \
        .option("kafka.bootstrap.servers", kafka_route) \
        .option("topic", "topic_all") \
        .start()
    
    query_aux = filtro_minutos(values, filters["2"])
 
    query_filter = query_aux \
        .withColumn("id", expr("uuid()")) \
        .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .queryName("FilterQuery") \
        .format("kafka") \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/spark/checkpoint2") \
        .option("kafka.bootstrap.servers", kafka_route) \
        .option("topic", "topic_filter") \
        .start()

    values3 = filtro_radio(values, filters["3"])

    query_radio = values3 \
        .withColumn("id", expr("uuid()")) \
        .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .queryName("RadioQuery") \
        .format("kafka") \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark/checkpoint3") \
        .option("kafka.bootstrap.servers", kafka_route) \
        .option("topic", "topic_radio") \
        .start()

    values4 = obtener_paradas(values, spark, filters["4"])

    query4 = values4 \
        .withColumn("id", expr("uuid()")) \
        .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark/checkpoint4") \
        .option("kafka.bootstrap.servers", kafka_route) \
        .option("topic", "topic4") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main("data/")
