from timeit import default_timer as timer
from datetime import timedelta
from functools import reduce
from operator import add

from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import Bucketizer

from helpers.transformer import transform
from helpers.saver import save


SPLITS = [
    0,
    1500,
    2000,
    2500,
    9999,
]

LABELS = (
    'Below1500',
    '1500-2000',
    '2000-2500',
    'Above2500',
)


def extract(df: DataFrame):
    start = timer()
    df = df.\
        filter(
            (f.col('BlackElo').isNotNull()) &
            (f.col('WhiteElo').isNotNull())
        )
    average = sum(x for x in [f.col('WhiteElo'), f.col('BlackElo')]) / 2
    df = df.\
        withColumn(
            'Elo',
            average,
        )

    buckets = Bucketizer(
        splits=SPLITS,
        inputCol='Elo',
        outputCol='EloRangeID',
    )
    df = buckets.transform(df)
    labels = f.array(
        *(f.lit(label) for label in LABELS)
    )

    df = df.\
        withColumn(
            'EloRange',
            labels.getItem(f.col('EloRangeID').cast('integer'))
        ).\
        groupBy('Opening').\
        pivot('EloRange').\
        count().\
        na.\
        fill(0)

    df = df.\
        withColumn(
            'Total',
            reduce(
                add,
                [f.col(x) for x in df.columns[1:]],
            )
        ). \
        orderBy(
            f.desc(
                f.col('Total')
            )
        ). \
        limit(10)

    print(f'Extracting: {timedelta(seconds=timer() - start)}')
    return df


def main():
    query = 'openings'
    spark = SparkSession.builder.appName(query).getOrCreate()
    data = spark.read.text('datasets/93mb.pgn')
    df = transform(data)
    df = extract(df)
    # df.show(truncate=False)
    save(df, query)
    spark.stop()


if __name__ == '__main__':
    main()
