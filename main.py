from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from analytical_queries import find_top_10_movies, find_letest_released_movies
from movie.spark_sql import create_tables

if __name__ == '__main__':
    spark = SparkSession.builder.enableHiveSupport().appName('Example 1').master('local[3]').getOrCreate()

    # Define the schema for movies
    movie_schema = StructType([
        StructField("movieId", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("genre", StringType(), True)
    ])
    movie_df = spark.read.option("delimiter", "::").schema(movie_schema).csv('data_wherehouse/ml-1m/movies.dat')
    movie_df.printSchema()
    movie_df.show(truncate=False)

    # define Schema for the Rating Data Frame UserID::MovieID::Rating::Timestamp
    rating_schema = StructType(
        [
            StructField('UserID', IntegerType(), True),
            StructField('MovieID', IntegerType(), True),
            StructField('Rating', IntegerType(), True),
            StructField('Timestamp', StringType(), True)
        ]
    )


    rating_df = spark.read.option('delimiter', '::').schema(rating_schema).csv('data_wherehouse/ml-1m/ratings.dat')
    rating_df = rating_df.withColumn("Timestamp", from_unixtime("Timestamp").cast("timestamp"))
    rating_df.printSchema()
    rating_df.show()

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
    user_df = spark.read.option('delimiter', '::').schema(user_schema).csv('data_wherehouse/ml-1m/users.dat')
    user_df.printSchema()
    user_df.show(truncate=False)

    # Find top 10 most viewed movies
    top_10_movies = find_top_10_movies(movie_df, rating_df)
    top_10_movies.show(truncate= False)

    # Find the distinct list of genre available

    distinct_genre = movie_df.select('genre').distinct()
    distinct_genre.show(truncate=False)


    # Find the movie count based on the genre
    movie_count = movie_df.groupBy("genre").agg(count("*").alias("movie Count"))
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
