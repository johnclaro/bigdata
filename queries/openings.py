from timeit import default_timer as timer
from datetime import timedelta
from functools import reduce
from operator import add

from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import Bucketizer

from helpers import transform, show_or_save

SPLITS = [
    0,
    1200,
    1400,
    1600,
    1800,
    2000,
    2200,
    2300,
    2400,
    2500,
    2700,
    9999,
]

LABELS = (
    '0-1200',
    '1200-1400',
    '1400-1600',
    '1600-1800',
    '1800-2000',
    '2000-2200',
    '2200-2300',
    '2300-2400',
    '2400-2500',
    '2500-2700',
    '2700+',
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
    filename = '93mb.pgn'
    spark = SparkSession.builder.appName('openings').getOrCreate()
    data = spark.read.text(f'datasets/{filename}')
    df = transform(data)
    df = extract(df)
    show_or_save(df, filename, 'openings')
    spark.stop()


if __name__ == '__main__':
    main()
