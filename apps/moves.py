from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

from helpers.timer import timer


def reformat(partition):
    for row in partition:
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

            yield [white, black]


@timer
def get_turns(df: DataFrame):
    df = df. \
        select('Notations').\
        rdd. \
        mapPartitions(reformat). \
        toDF(['WhitePly', 'BlackPly'])

    return df


def extract(df: DataFrame):
    df = get_turns(df)
    return df
