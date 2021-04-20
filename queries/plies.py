from timeit import default_timer as timer
from datetime import timedelta

from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from helpers import transform, show_or_save


def extract(df: DataFrame):
    start = timer()
    df = df.\
        withColumn(
            'Plies',
            f.split(
                'moves',
                ' '
            )[0]
        )

    print(f'Extracting: {timedelta(seconds=timer() - start)}')
    return df


def main():
    query = 'plies'
    spark = SparkSession.builder.appName(query).getOrCreate()
    data = spark.read.text('datasets/test.pgn')
    df = transform(data)
    df = extract(df)
    show_or_save(df, query, 'save')
    spark.stop()


if __name__ == '__main__':
    main()
