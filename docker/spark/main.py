from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import expr, element_at, split, input_file_name
import os

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
    # query = values \
    #     .writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .start()

    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main("data/")