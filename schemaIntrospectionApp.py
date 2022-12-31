import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, concat


def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path


def main(spark):
    filename = 'Restaurants_in_Wake_County_NC.csv'
    path = 'data/'
    absolute_file_path = get_absolute_file_path(path, filename)
    df = spark.read.csv(header=True, inferSchema=True, path=absolute_file_path)

    df = df.withColumn("county", lit("Wake")) \
        .withColumnRenamed("HSISID", "datasetId") \
        .withColumnRenamed("NAME", "name") \
        .withColumnRenamed("ADDRESS1", "address1") \
        .withColumnRenamed("ADDRESS2", "address2") \
        .withColumnRenamed("CITY", "city") \
        .withColumnRenamed("STATE", "state") \
        .withColumnRenamed("POSTALCODE", "zip") \
        .withColumnRenamed("PHONENUMBER", "tel") \
        .withColumnRenamed("RESTAURANTOPENDATE", "dateStart") \
        .withColumnRenamed("FACILITYTYPE", "type") \
        .withColumnRenamed("X", "geoX") \
        .withColumnRenamed("Y", "geoY")

    df = df.withColumn("id",
                       concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))

    print("*** Schema as a tree:")
    df.printSchema()

    print("*** Schema as string: {}".format(df.schema))
    schema_as_json = df.schema.json()
    parsed_schema_as_json = json.loads(schema_as_json)

    print("*** Schema as JSON: {}".format(json.dumps(parsed_schema_as_json, indent=2)))


if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder \
        .appName("Schema introspection for restaurants in Wake County, NC") \
        .master("local[*]").getOrCreate()

    main(spark)
    # Good to stop SparkSession at the end of the application
    spark.stop()
