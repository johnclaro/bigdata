from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame


def extract(df: DataFrame):
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

    return df
