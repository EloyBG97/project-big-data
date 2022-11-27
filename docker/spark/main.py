from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import expr, element_at, split, input_file_name, current_timestamp, minute, collect_list
import os
import json

scala_version = '2.12'
spark_version = '3.2.2'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.2'
]
data_route = os.environ.get('dataRoute')
kafka_route = os.environ.get('kafkaRoute')
print("KAFKA ROUTE:")
print(kafka_route)

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

    # lines.printSchema()

    values = lines

    # values.printSchema()

    # Start running the query that prints the output in the screen
       
    query = values \
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

    query_aux = query_aux.groupby(values.columns).agg(collect_list('codBus').alias('dummy')).drop('dummy')
 
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
    
    """
    query = query_aux \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()
    """

    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main("data/")
