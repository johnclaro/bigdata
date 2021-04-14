from pyspark.sql import SparkSession, Row


def main():
    spark = SparkSession.builder.appName('SparkSQL').getOrCreate()
    chess = spark.\
        read.\
        option('header', 'true').\
        option('inferSchema', 'true').\
        csv('chess/files/jan2013.csv')

    chess.printSchema()
    chess.select('*').show()
    spark.stop()


if __name__ == '__main__':
    main()
