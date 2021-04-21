from functools import reduce
from operator import add

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import Bucketizer


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

    return df
