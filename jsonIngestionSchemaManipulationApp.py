import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, split, concat


def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path


def main(spark):
    filename = 'Restaurants_in_Durham_County_NC.json'
    path = 'data/'
    absolute_file_path = get_absolute_file_path(path, filename)

    df = spark.read.json(absolute_file_path)
    print("*** Right after ingestion")
    df.show(5)
    df.printSchema()
    print("We have {} records.".format(df.count))

    df = df.withColumn("county", lit("Durham")) \
        .withColumn("datasetId", col("fields.id")) \
        .withColumn("name", col("fields.premise_name")) \
        .withColumn("address1", col("fields.premise_address1")) \
        .withColumn("address2", col("fields.premise_address2")) \
        .withColumn("city", col("fields.premise_city")) \
        .withColumn("state", col("fields.premise_state")) \
        .withColumn("zip", col("fields.premise_zip")) \
        .withColumn("tel", col("fields.premise_phone")) \
        .withColumn("dateStart", col("fields.opening_date")) \
        .withColumn("dateEnd", col("fields.closing_date")) \
        .withColumn("type", split(col("fields.type_description"), " - ").getItem(1)) \
        .withColumn("geoX", col("fields.geolocation").getItem(0)) \
        .withColumn("geoY", col("fields.geolocation").getItem(1))

    df = df.withColumn("id", concat(col("state"), lit("_"),
                                    col("county"), lit("_"),
                                    col("datasetId")))

    print("*** Dataframe transformed")
    df.select('id', "state", "county", "datasetId").show(5)
    df.printSchema()

    print("*** Looking at partitions")
    partition_count = df.rdd.getNumPartitions()
    print("Partition count before repartition: {}".format(partition_count))

    df = df.repartition(4)
    print("Partition count after repartition: {}".format(df.rdd.getNumPartitions()))


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Restaurants in Durham County, NC") \
        .master("local[*]").getOrCreate()

    main(spark)
    spark.stop()
