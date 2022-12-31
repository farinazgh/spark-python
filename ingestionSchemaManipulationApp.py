import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col


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

    print("*** Right after ingestion")
    df.show(5)
    df.printSchema()

    print("We have {} records.".format(df.count()))

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
        .withColumnRenamed("Y", "geoY") \
        .drop("OBJECTID", "PERMITID", "GEOCODESTATUS")

    df = df.withColumn("id",
                       concat(col("state"), lit("_"),
                              col("county"), lit("_"),
                              col("datasetId")))

    # Shows at most 5 rows from the dataframe
    print("*** Dataframe transformed")
    df.show(5)

    # for book only
    df_used_for_book = df.drop("address2", "zip", "tel", "dateStart", "geoX", "geoY", "address1", "datasetId")

    df_used_for_book.show(5, 15)
    # end

    df.printSchema()

    print("*** Looking at partitions")
    partition_count = df.rdd.getNumPartitions()
    print("Partition count before repartition: {}".format(partition_count))

    df = df.repartition(4)

    print("Partition count after repartition: {}".format(df.rdd.getNumPartitions()))


if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Restaurants in Wake County, NC") \
        .master("local[*]").getOrCreate()

    main(spark)
    spark.stop()
