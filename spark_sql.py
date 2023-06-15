from pyspark.sql.functions import *


def create_tables(spark, movie_df, rating_df, user_df):

    movie_df.createOrReplaceTempView("movies")
    user_df.createOrReplaceTempView("users")
    rating_df.createOrReplaceTempView("ratings")

    spark.sql("CREATE TABLE IF NOT EXISTS movies_table AS SELECT * FROM movies")

    spark.sql("CREATE TABLE IF NOT EXISTS users_table AS SELECT * FROM users")

    spark.sql("CREATE TABLE IF NOT EXISTS ratings_table AS SELECT * FROM ratings")


def find_oldest_movies(spark, movie_df):
    year = regexp_extract(movie_df["Title"], r"\((\d{4})\)", 1)
    movies_with_year = movie_df.withColumn("year", year)
    movies_with_year.createOrReplaceTempView("movies")
    oldest_movies = spark.sql("""
        SELECT m.title, m.year
        FROM movies AS m
        WHERE m.year IS NOT NULL
        ORDER BY m.year
        LIMIT 10
    """)
    return oldest_movies


def find_movies_released_each_year(spark, movie_df):
    year = regexp_extract(movie_df["Title"], r"\((\d{4})\)", 1)
    movies_with_year = movie_df.withColumn("year", year)
    movies_with_year.createOrReplaceTempView("movies")

    movie_count = spark.sql('''
        SELECT year, COUNT(*) AS movie_count
        FROM movies
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
        
    ''')
    return movie_count


def find_movie_count_by_rating(spark, rating_df):

    rating_df.createOrReplaceTempView('ratings')

    movie_count_based_on_rating = spark.sql(
        '''
        SELECT Rating, COUNT(*) AS movie_count
        FROM ratings
        WHERE MovieID IS NOT NULL
        GROUP BY Rating
        ORDER BY Rating
        '''
    )
    return movie_count_based_on_rating


def find_user_count_based_on_movie(spark, rating_df, movie_df):
    # Create a temporary view for the DataFrame
    rating_df.createOrReplaceTempView("ratings")
    movie_df.createOrReplaceTempView('movies')

    # Execute SQL query to count users who have rated each movie
    users_per_movie = spark.sql("""
        SELECT movies.Title, COUNT(DISTINCT ratings.UserID) AS user_count
        FROM ratings
        JOIN movies
        ON ratings.movieID = movies.movieId
        GROUP BY  movies.Title
    """)

    return users_per_movie


def find_total_rating_for_movie(spark, rating_df, movie_df):
    rating_df.createOrReplaceTempView('ratings')
    movie_df.createOrReplaceTempView('movies')

    total_movie_rating = spark.sql(
        '''
        SELECT movies.Title, COUNT(ratings.rating) AS total_rating
        FROM ratings
        JOIN movies
        on ratings.movieID = movies.movieId
        GROUP BY movies.Title
        ORDER BY total_rating DESC
        '''
    )
    return total_movie_rating


def find_avg_rating_for_movie(spark, rating_df, movie_df):
    rating_df.createOrReplaceTempView('ratings')
    movie_df.createOrReplaceTempView('movies')

    total_movie_rating = spark.sql(
        '''
        SELECT movies.Title, AVG(ratings.rating) AS average_rating
        FROM ratings
        JOIN movies
        on ratings.movieID = movies.movieId
        GROUP BY movies.Title
        '''
    )
    return total_movie_rating
