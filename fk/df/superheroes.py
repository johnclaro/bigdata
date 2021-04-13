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

    most_connections = query.\
        sort(
            col('connections').desc(),
        ).\
        first()

    most_names = names.\
        filter(
            col('id') == most_connections[0]
        ).\
        select('name').\
        first()

    print(f'{most_names[0]} is the most popular '
          f'superhero with {most_connections[1]}')

    spark.stop()


if __name__ == '__main__':
    main()
