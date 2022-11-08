from pyspark.sql import SparkSession, functions
import sys
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
import time
import urllib.request
import json


def main() -> None:
    """ Program that reads parkings info in streaming from a URL,
    and shows the name, date (YY/MM/DD HH:MM), capacity and free spaces by each parking.
    At least two cores are needed: one to run the main program and one to poll the streaming directory
    :param directory: streaming directory
    """
    directory = "csv/inlive"

    spark_session = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("StreamingBusInfoRealTime") \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    fields = [StructField("codBus", IntegerType(), True),
              StructField("long", DoubleType(), True),
              StructField("last_update", TimestampType(), True),
              StructField("lat", DoubleType(), True),
              StructField("codLinea", IntegerType(), True),
              StructField("sentido", IntegerType(), True),
              StructField("codParIni", IntegerType(), True)
              ]

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark_session \
        .readStream \
        .format("csv") \
        .options(header='true') \
        .schema(StructType(fields)) \
        .load(directory)

    lines.printSchema()
'''
    # Mostrar los 3 campos
    listValues = ["None", "-1"]
    values = lines.select("nombre","capacidad","libres")\
        .filter(lines.libres != -1)\
        .filter(lines.capacidad != -1)\
        .filter(lines.libres != "None")\
        .filter(lines.capacidad != "None")

    query = values \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    query.awaitTermination()
'''

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Revise el programa. Algo no ha salido correctamente. ", file=sys.stderr)
        exit(-1)
    main()