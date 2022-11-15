from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import *



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
              StructField("codLinea", IntegerType(), True),
              StructField("sentido", IntegerType(), True),
              StructField("long", DoubleType(), True),
              StructField("lat", DoubleType(), True),
              StructField("codParIni", IntegerType(), True),
              StructField("last_update", TimestampType(), True)
              ]

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark_session \
        .readStream \
        .format("csv") \
        .options(header='true') \
        .schema(StructType(fields)) \
        .load(directory)

    lines.printSchema()

    # Mostrar los 3 campos
    listValues = ["None", "-1"]
    values = lines.select("long","last_update","lat","codParIni")

    query = values \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    query.awaitTermination()
'''
\
        .filter(lines.libres != -1)\
        .filter(lines.capacidad != -1)\
        .filter(lines.libres != "None")\
        .filter(lines.capacidad != "None")
'''



if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Revise el programa. Algo no ha salido correctamente. ", file=sys.stderr)
        exit(-1)
    main()