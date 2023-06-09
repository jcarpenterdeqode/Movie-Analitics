from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Example 1').master('local[3]').getOrCreate()

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



