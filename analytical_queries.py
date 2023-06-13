from pyspark.sql.functions import *


def find_top_10_movies(movie_df, rating_df):

    rating_count = rating_df.groupBy('MovieID').count()
    rating_count.show()
    joined_df = movie_df.join(rating_count, 'MovieID')
    sorted_df = joined_df.orderBy(col("count").desc())
    top_10_movies = sorted_df.select("title", "count").limit(10)

    return top_10_movies