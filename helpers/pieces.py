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
        locations = []
        for index in range(0, len(row.Notations), 2):
            white = row.Notations[index]
            try:
                black = row.Notations[index + 1]
            except IndexError:
                black = None

            if white == 'O-O':
                if piece == 'K':
                    white = 'Kg1'
                elif piece == 'R':
                    white = 'Rf1'
            elif white == 'O-O-O':
                if piece == 'K':
                    white = 'Kc1'
                elif piece == 'R':
                    white = 'Rd1'
            elif black == 'O-O':
                if piece == 'K':
                    black = 'Kg8'
                elif piece == 'R':
                    black = 'Rf8'
            elif black == 'O-O-O':
                if piece == 'K':
                    black = 'Kc8'
                elif piece == 'R':
                    black = 'Rd8'

            if white.startswith(piece):
                white = remove_symbols(white)[-2:]
                locations.append(white)
            elif black and black.startswith(piece):
                black = remove_symbols(black)[-2:]
                locations.append(black)

        yield [locations]


@timer
def find_pieces(df: DataFrame, piece):
    df = df. \
        select('Notations').\
        rdd. \
        mapPartitions(lambda partition: map_notations(partition, piece)). \
        toDF(['Location'])

    return df


@timer
def group_by_location(df: DataFrame):
    df = df.\
        withColumn(
            'Location',
            f.explode('Location')
        ).\
        groupBy('Location').\
        count().withColumnRenamed('count', 'Count')

    return df
