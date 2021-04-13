from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName('Movies').getOrCreate()

    spark.stop()


if __name__ == '__main__':
    main()
