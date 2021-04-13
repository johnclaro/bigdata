from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    LongType,
)
from pyspark.sql.functions import desc


def main():
    spark = SparkSession.builder.appName('Movies').getOrCreate()
    schema = StructType([
        StructField('user_id', IntegerType(), True),
        StructField('movie_id', IntegerType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', LongType(), True)
    ])
    csv = 'fk/files/movies/u.data'
    df = spark.read.option('sep', '\t').schema(schema).csv(csv)
    df.\
        groupBy('movie_id').\
        count().\
        orderBy(
            desc('count'),
        ).\
        show(10)

    spark.stop()


if __name__ == '__main__':
    main()
