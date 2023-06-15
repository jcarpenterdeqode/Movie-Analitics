from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from analytical_queries import find_top_10_movies, find_letest_released_movies
from spark_sql import (
    create_tables,
    find_oldest_movies,
    find_movies_released_each_year,
    find_movie_count_by_rating,
    find_user_count_based_on_movie,
    find_total_rating_for_movie,
    find_avg_rating_for_movie,
)
import logging


# Set up logging configuration
logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    spark = SparkSession.builder.enableHiveSupport().appName('Example 1').master('local[3]').getOrCreate()

    # Define the schema for movies
    movie_schema = StructType([
        StructField("movieId", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("genre", StringType(), True)
    ])
    movie_df = spark.read.option("delimiter", "::") \
        .schema(movie_schema) \
        .csv('data_wherehouse/ml-1m/movies.dat')

    # movie_df.printSchema()
    # movie_df.show(truncate=False)

    # define Schema for the Rating Data Frame UserID::MovieID::Rating::Timestamp
    rating_schema = StructType(
        [
            StructField('UserID', IntegerType(), True),
            StructField('MovieID', IntegerType(), True),
            StructField('Rating', IntegerType(), True),
            StructField('Timestamp', StringType(), True)
        ]
    )

    rating_df = spark.read.option('delimiter', '::') \
        .schema(rating_schema) \
        .csv('data_wherehouse/ml-1m/ratings.dat')

    rating_df = rating_df.withColumn("Timestamp", from_unixtime("Timestamp").cast("timestamp"))
    # rating_df.printSchema()
    # rating_df.show()

    # define Schema for the User data Frame UserID::Gender::Age::Occupation::Zip-code
    user_schema = StructType(
        [
            StructField('UserID', IntegerType(), False),
            StructField('Gender', StringType(), True),
            StructField('Age', IntegerType(), True),
            StructField('Occupation', StringType(), True),
            StructField('Zip-code', StringType(), True),
        ]
    )

    # Read file from the .dat file and create a data frame
    user_df = spark.read.option('delimiter', '::') \
        .schema(user_schema) \
        .csv('data_wherehouse/ml-1m/users.dat')

    # user_df.printSchema()
    # user_df.show(truncate=False)

    # Find top 10 most viewed movies
    top_10_movies = find_top_10_movies(movie_df, rating_df)
    logging.info("Top 10 movies based on ratings")
    top_10_movies.show(truncate=False)

    # Find the distinct list of genre available

    distinct_genre = movie_df.select('genre').distinct()
    logging.info("distinct Genre find by Genre")
    distinct_genre.show(truncate=False)


    # Find the movie count based on the genre
    movie_count = movie_df.groupBy("genre").agg(count("*").alias("movie Count"))
    logging.info("Based on genre count of movies")
    movie_count.show()

    # Find the movies that start with numbers or latter
    movie_counts = movie_df.withColumn("start_char", regexp_extract("Title", "^(\\d+|[A-Z])", 1)) \
    .groupBy("start_char").agg(count("*").alias("count"))
    movie_counts.show()

    # find the letest movies
    latest_movies = find_letest_released_movies(movie_df)
    latest_movies.show(10, truncate=False)

    # create Tables
    create_tables(spark, movie_df, rating_df, user_df)

    # find oldest movies
    oldest_movies = find_oldest_movies(spark, movie_df)
    oldest_movies.show(truncate=False)

    # find the movies count based on year
    movie_count = find_movies_released_each_year(spark, movie_df)
    movie_count.show()

    # find the movies count based on rating
    movie_count = find_movie_count_by_rating(spark, rating_df)
    movie_count.show()

    # How many users have rated each movie?
    user_count = find_user_count_based_on_movie(spark, rating_df, movie_df)
    user_count.show(truncate=False)

    # What is the total rating for each movie?

    total_ratings = find_total_rating_for_movie(spark, rating_df, movie_df)
    total_ratings.show(truncate= False)

    # What is the average rating for each movie?
    avg_rating = find_avg_rating_for_movie(spark, rating_df, movie_df)
    avg_rating.show(truncate=False)

    # Save table without defining DDL in Hive
    movie_df.write.format('parquet').mode('overwrite').saveAsTable('movie_table_dataframe')
    rating_df.write.format('parquet').mode('overwrite').saveAsTable('rating_table_dataframe')
    user_df.write.format('parquet').mode('overwrite').saveAsTable('user_table_dataframe')
