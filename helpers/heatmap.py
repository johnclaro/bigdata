from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

from helpers.timer import timer


def remove_symbols(ply: str):
    return ply.\
        replace('+', '').\
        replace('?', '').\
        replace('+', '').\
        replace('#', '').\
        replace('x', '').\
        replace('!', '')


def map_notations(partition, piece):
    for row in partition:
        pieces = []
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

            if white.startswith(piece):
                white = remove_symbols(white)[-2:]
                pieces.append(white)
            elif black and black.startswith(piece):
                black = remove_symbols(black)[-2:]
                pieces.append(black)

        yield [pieces]


@timer
def get_piece(df: DataFrame, piece, column):
    df = df. \
        select('Notations').\
        rdd. \
        mapPartitions(lambda partition: map_notations(partition, piece)). \
        toDF([column])

    return df


@timer
def group_by_piece(df: DataFrame, column):
    df = df.\
        withColumn(
            column,
            f.explode(column)
        ).\
        groupBy(column).\
        count().withColumnRenamed('count', 'Count')

    return df
