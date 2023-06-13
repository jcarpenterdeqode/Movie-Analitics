from pyspark.sql.functions import *


def create_tables(spark, movie_df, rating_df, user_df):

    movie_df.createOrReplaceTempView("movies")
    user_df.createOrReplaceTempView("users")
    rating_df.createOrReplaceTempView("ratings")

    spark.sql("CREATE TABLE IF NOT EXISTS movies_table AS SELECT * FROM movies")

    spark.sql("CREATE TABLE IF NOT EXISTS users_table AS SELECT * FROM users")

    spark.sql("CREATE TABLE IF NOT EXISTS ratings_table AS SELECT * FROM ratings")
