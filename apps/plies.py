from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

from helpers.timer import timer


@timer
def get_notations_size(df: DataFrame):
    df = df. \
        filter(
            (f.col('Result') != '')
        ). \
        withColumn(
            'Plies',
            f.size('Notations')
        )
    return df


@timer
def pivot_result(df: DataFrame):
    df = df. \
        groupBy('Plies', 'Result'). \
        pivot('Result'). \
        count()
    return df


@timer
def group_by_plies(df: DataFrame):
    df = df. \
        groupBy('Plies'). \
        agg(
            f.sum('0-1').alias('BlackWins'),
            f.sum('1-0').alias('WhiteWins'),
            f.sum('1/2-1/2').alias('Draw'),
        ). \
        na. \
        fill(0)
    return df


def extract(df: DataFrame):
    df = get_notations_size(df)
    df = pivot_result(df)
    df = group_by_plies(df)
    return df
