from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import expr, element_at, split, input_file_name, \
    countDistinct, col, udf, max, min, concat_ws, current_timestamp, minute
import os
from enum import Enum
import json

scala_version = '2.12'
spark_version = '3.2.2'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.2'
]
data_route = os.environ.get('dataRoute')
kafka_route = os.environ.get('kafkaRoute')


class ColumnName(Enum):
    COD_LINEA = "codLinea"
    LON = "lon"
    LAT = "lat"
    COD_PARADA = "codParada"
    ORDEN = "orden"


def obtener_paradas(values, spark):
    df = spark \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("lineasyparadas.csv")

    parada1 = "1555"
    parada2 = "1556"
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
        StructField("last_update", TimestampType(), True)
    ]

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("csv") \
        .options(header='true') \
        .schema(StructType(fields)) \
        .load(directory) \
        .withColumn("filename", element_at(split(input_file_name(), "/"), -1)) \
        .withColumn("timestamp", element_at(split("filename", ".txt"), 1))

    values = lines
    
    # Start running the query that prints the output in the screen
    query1 = values \
        .withColumn("id", expr("uuid()")) \
        .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark/checkpoint") \
        .option("kafka.bootstrap.servers", kafka_route) \
        .option("topic", "topic_test") \
        .start()

    # Filtering Query    
    filter_2 = filters["2"]
    query_aux = lines
    
    if("codLinea" in filter_2.keys()):
        query_aux = query_aux.filter(values["codLinea"] == filter_2["codLinea"])

    if("sentido" in filter_2.keys()):
        query_aux = query_aux.filter(values["sentido"] == filter_2["sentido"])

    if("last_update" in filter_2.keys()): 
        query_aux = query_aux.filter(minute(current_timestamp()) - minute(values["last_update"]) < filter_2["last_update"])      

    query_filter = query_aux \
        .withColumn("id", expr("uuid()")) \
        .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .queryName("FilterQuery") \
        .format("kafka") \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark/checkpoint2") \
        .option("kafka.bootstrap.servers", kafka_route) \
        .option("topic", "topic_filter") \
        .start()
    
    """
    query = query_aux \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()
    """

    values4 = obtener_paradas(values, spark)

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
