from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

from helpers.timer import timer


@timer
def get_plies(df: DataFrame):
    df = df. \
        withColumn(
            'Plies',
            f.explode('Notations')
        ). \
        filter(
            f.col('Plies').like('K%')
        )
    return df


@timer
def get_king_plies(df: DataFrame):
    df = df.\
        withColumn(
            'KingPly',
            f.regexp_replace(
                f.split(
                    f.regexp_replace(
                        f.col('Plies'),
                        'Kx',
                        'K'
                    ),
                    'K'
                )[1],
                r'[^\w\*]',
                ''
            )
        )
    return df


@timer
def group_by_kings(df: DataFrame):
    df = df.\
        groupBy('KingPly').\
        count().withColumnRenamed('count', 'Count')
    return df


def extract(df: DataFrame):
    df = get_plies(df)
    df = get_king_plies(df)
    df = group_by_kings(df)
    return df
