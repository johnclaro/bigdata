from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

from helpers.timer import timer

numbers = ['1', '2', '3', '4', '5', '6', '7', '8']
alphabets = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']


def update_locations(locations, notation, piece):
    if notation:
        is_pawn = piece == 'P' and notation[0] in alphabets
        if notation.startswith(piece) or is_pawn:
            if is_pawn and '=' in notation:
                notation = notation.split('=')[0]

            location = remove_symbols(notation)[-2:]
            locations.append(location)


def get_castle_notation(color, notation, piece):
    castles = {
        'white': {
            'O-O': {'K': 'Kg1', 'R': 'Rf1'},
            'O-O-O': {'K': 'Kc1', 'R': 'Rd1'},
        },
        'black': {
            'O-O': {'K': 'Kg8', 'R': 'Rf8'},
            'O-O-O': {'K': 'Kc8', 'R': 'Rd8'},
        },
    }

    try:
        notation = castles[color][notation][piece]
    except KeyError:
        pass

    return notation


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
            white_notation = row.Notations[index]
            try:
                black_notation = row.Notations[index + 1]
            except IndexError:
                black_notation = None

            white_notation = get_castle_notation('white', white_notation, piece)
            black_notation = get_castle_notation('black', black_notation, piece)

            update_locations(locations, white_notation, piece)
            update_locations(locations, black_notation, piece)

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
