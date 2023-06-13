from pyspark.sql.functions import *


def find_top_10_movies(movie_df, rating_df):

    rating_count = rating_df.groupBy('MovieID').count()
    rating_count.show()
    joined_df = movie_df.join(rating_count, 'MovieID')
    sorted_df = joined_df.orderBy(col("count").desc())
    top_10_movies = sorted_df.select("title", "count").limit(10)

    return top_10_movies


def find_letest_released_movies(movie_df):
    year = regexp_extract(movie_df["Title"], r"\((\d{4})\)", 1)
    movies_with_year = movie_df.withColumn("year", year)
    sorted_movies = movies_with_year.select('movieId', 'title', 'genre').orderBy(desc("year"))
    return sorted_movies
