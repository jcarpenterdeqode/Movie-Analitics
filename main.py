from pyspark.sql import SparkSession

if __name__ == '__main__':
    

    spark = SparkSession.Builder.appName('Movie Analitics').getOrCreate()

    text_read = spark.read.csv('/data_wherehouse/ml-1m/movies.dat')

    text_read.show()