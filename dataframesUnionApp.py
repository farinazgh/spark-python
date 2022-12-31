import os

from pyspark.sql import SparkSession

import util


def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path


def main(spark):
    filename1 = 'Restaurants_in_Wake_County_NC.csv'
    path1 = 'data/'
    absolute_file_path1 = get_absolute_file_path(path1, filename1)

    filename2 = 'Restaurants_in_Durham_County_NC.json'
    path2 = 'data/'
    absolute_file_path2 = get_absolute_file_path(path2, filename2)

    df1 = spark.read.csv(path=absolute_file_path1, header=True, inferSchema=True)

    df2 = spark.read.json(absolute_file_path2)

    wake_restaurants_df = util.build_wake_restaurants_dataframe(df1)
    durham_restaurants_df = util.build_durham_restaurants_dataframe(df2)

    util.combine_dataframes(wake_restaurants_df, durham_restaurants_df)


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Union of two dataframes") \
        .master("local[*]").getOrCreate()

    main(spark)
    spark.stop()
