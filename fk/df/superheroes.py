from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, sum
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
)


def main():
    spark = SparkSession.builder.appName('Superheroes').getOrCreate()

    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
    ])

    whitespace = ' '
    graph_file = 'fk/files/superheroes/graph.txt'
    names_file = 'fk/files/superheroes/names.txt'
    names = spark.read.schema(schema).option('sep', whitespace).csv(names_file)
    graph = spark.read.text(graph_file)

    query = graph.\
        withColumn(
            'id',
            split(
                col('value'),
                whitespace
            )[0]
        ).\
        withColumn(
            'connections',
            size(
                split(
                    col('value'),
                    whitespace
                ),
            ) - 1,
        ).\
        groupBy('id').\
        agg(
            sum('connections').alias('connections')
        )\

    most_popular = query.\
        sort(
            col('connections').desc(),
        ).\
        first()

    most_popular_name = names.\
        filter(
            col('id') == most_popular[0]
        ).\
        select('name').\
        first()

    name = most_popular_name[0]
    connections = most_popular[1]
    print(f'{name} is the most popular superhero with {connections}')

    spark.stop()


if __name__ == '__main__':
    main()
