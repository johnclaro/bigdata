from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

from helpers.timer import timer


def map_turns(partition):
    for row in partition:
        kings = []
        for index in range(0, len(row.Notations), 2):
            white = row.Notations[index]
            try:
                black = row.Notations[index + 1]
            except IndexError:
                black = None

            if white == 'O-O':
                white = 'Kg1'
            elif white == 'O-O-O':
                white = 'Kc1'
            elif black == 'O-O':
                black = 'Kg8'
            elif black == 'O-O-O':
                black = 'Kc8'

            if 'K' in white:
                white = white.replace('x', '')
                kings.append(white[1:3])
            elif black and 'K' in black:
                black = black.replace('x', '')
                kings.append(black[1:3])

        yield [kings]


@timer
def get_turns(df: DataFrame):
    df = df. \
        select('Notations').\
        rdd. \
        mapPartitions(map_turns). \
        toDF(['Kings'])

    return df


@timer
def group_by_kings(df: DataFrame):
    df = df.\
        withColumn(
            'Kings',
            f.explode('Kings')
        ).\
        groupBy('Kings').\
        count().withColumnRenamed('count', 'Count')

    return df


def extract(df: DataFrame):
    df = get_turns(df)
    df = group_by_kings(df)
    return df
