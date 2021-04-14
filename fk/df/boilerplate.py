from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
)


def main():
    spark = SparkSession.builder.appName('Superheroes').getOrCreate()



    spark.stop()


if __name__ == '__main__':
    main()
