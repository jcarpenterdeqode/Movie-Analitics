from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()
    counter = spark.sparkContext.accumulator(0)
    data = [(1, 'apple'), (2, 'banana'), (3, 'apple'), (4, 'banana')]
    df = spark.createDataFrame(data, ['id', 'fruit'])

    def count_apples(row):
        global counter
        if row['fruit'] == 'apple':
            counter += 1

    df.foreach(count_apples)

    # Print the final count
    print("Number of apples:", counter.value)
