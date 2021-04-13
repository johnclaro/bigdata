from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, sum, min
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
        )

    least_connections = query.agg(min('connections')).first()
    superheroes = query.\
        filter(
            col('connections') == least_connections[0]
        ).\
        join(names, 'id').\
        select('name')

    print(f'The following superheroes only '
          f'have {least_connections[0]} connection(s):')
    for index, superhero in enumerate(superheroes.collect()):
        print(f"\t {index + 1}) {superhero[0].title()}")

    spark.stop()


if __name__ == '__main__':
    main()
