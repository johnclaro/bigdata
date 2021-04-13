import codecs
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
)
from pyspark.sql.functions import udf, col, desc


def load_names() -> Dict[int, str]:
    names = {}
    with codecs.open('fk/files/movies/u.ITEM',
                     'r',
                     encoding='ISO-8859-1',
                     errors='ignore') as codecs_file:
        for line in codecs_file:
            fields = line.split('|')
            movie_id = int(fields[0])
            name = fields[1]
            names[movie_id] = name
    return names


def lookup(movie_id):
    return names.value[movie_id]


spark = SparkSession.builder.appName('Movies').getOrCreate()
names = spark.sparkContext.broadcast(load_names())


def main():
    schema = StructType([
        StructField('user_id', IntegerType(), True),
        StructField('movie_id', IntegerType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', LongType(), True)
    ])
    csv = 'fk/files/movies/u.data'
    df = spark.read.option('sep', '\t').schema(schema).csv(csv)
    lookup_udf = udf(lookup)
    df.\
        groupBy('movie_id').\
        count().\
        withColumn(
            'title',
            lookup_udf(
                col('movie_id')
            ),
        ).\
        orderBy(
            desc('count'),
        ).\
        show(10, False)

    spark.stop()


if __name__ == '__main__':
    main()
