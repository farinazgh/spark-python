from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


def main(spark):
    data = [['Rouly'], ['Tag'], ['Nini'], ['Betty'], ['Duggee'], ['Youpi']]
    schema = StructType([StructField('name', StringType(), True)])

    df = spark.createDataFrame(data, schema)
    df.show()
    df.printSchema()


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Array to Dataframe") \
        .master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel('warn')
    main(spark)
    spark.stop()
