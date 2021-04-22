from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

from helpers.timer import timer


@timer
def get_plies(df):
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
def get_king_plies(df):
    df = df.\
        withColumn(
            'Kings',
            f.split(
                f.regexp_replace(
                    f.col('Plies'),
                    'Kx',
                    'K'
                ),
                'K'
            )[1]
        )
    return df


def extract(df: DataFrame):
    df = get_plies(df)
    df = get_king_plies(df)
    df = df.select('Kings')
    return df
