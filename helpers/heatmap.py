from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

from helpers.timer import timer


def map_turns(partition, ply):
    for row in partition:
        plies = []
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

            if ply in white:
                white = white.replace('x', '')
                plies.append(white[1:3])
            elif black and ply in black:
                black = black.replace('x', '')
                plies.append(black[1:3])

        yield [plies]


@timer
def get_turns(df: DataFrame, half_move, column):
    df = df. \
        select('Notations').\
        rdd. \
        mapPartitions(lambda j: map_turns(j, half_move)). \
        toDF([column])

    return df


@timer
def group_by_ply(df: DataFrame, column):
    df = df.\
        withColumn(
            column,
            f.explode(column)
        ).\
        groupBy(column).\
        count().withColumnRenamed('count', 'Count')

    return df
