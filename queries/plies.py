from timeit import default_timer as timer
from datetime import timedelta

from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType

from helpers import transform, show_or_save


def extract(df: DataFrame):
    start = timer()
    df = df.\
        filter(
            (f.col('Result') != '')
        ).\
        withColumn(
            'Plies',
            f.size('Notations')
        ). \
        groupBy('Plies', 'Result'). \
        pivot('Result'). \
        count(). \
        groupBy('Plies'). \
        agg(
            f.sum('0-1').alias('BlackWins'),
            f.sum('1-0').alias('WhiteWins'),
            f.sum('1/2-1/2').alias('Draw'),
        ). \
        na. \
        fill(0)

    print(f'Extracting: {timedelta(seconds=timer() - start)}')
    return df


def main():
    query = 'plies'
    spark = SparkSession.builder.appName(query).getOrCreate()
    data = spark.read.text('datasets/93mb.pgn')
    df = transform(data)
    df = extract(df)
    show_or_save(df, query, 'save')
    spark.stop()


if __name__ == '__main__':
    main()
