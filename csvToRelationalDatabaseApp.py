from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

current_dir = os.path.dirname(__file__)
relative_path = "data/authors.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder\
    .appName("CSV to DB")\
    .config("spark.jars", "data/postgresql-42.5.1.jar")\
    .master("local")\
    .getOrCreate()


df = spark.read.csv(header=True, inferSchema=True, path=absolute_file_path)
df = df.withColumn("name", F.concat(F.col("lname"), F.lit(", "), F.col("fname")))
dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs"

prop = {"driver":"org.postgresql.Driver", "user":"postgres", "password":""}
df.write.jdbc(mode='overwrite', url=dbConnectionUrl, table="authors_p02", properties=prop)

spark.stop()
