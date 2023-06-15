from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession.builder \
            .master('local[4]') \
            .appName('Broadcast Variable Example') \
            .getOrCreate()

    states = {"NY": "New York", "CA": "California", "FL": "Florida"}
    broadcastStates = spark.sparkContext.broadcast(states)

    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")
            ]

    columns = ["firstname", "lastname", "country", "state"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    df.show(truncate=False)

    def state_convert(code):
        return broadcastStates.value[code]

    result = df.rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).toDF(columns)
    result.show(truncate=False)
