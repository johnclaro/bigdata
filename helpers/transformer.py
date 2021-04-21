from collections import deque

from pyspark.sql.dataframe import DataFrame

from helpers.timer import timer


schema = {
    'Event': '',
    'Site': '',
    'White': '',
    'Black': '',
    'Result': '',
    'UTCDate': '',
    'UTCTime': '',
    'WhiteElo': 0,
    'BlackElo': 0,
    'WhiteRatingDiff': '',
    'BlackRatingDiff': '',
    'WhiteTitle': '',
    'BlackTitle': '',
    'ECO': '',
    'Opening': '',
    'TimeControl': '',
    'Termination': '',
    'Notations': [],
}


def is_ply(value):
    return (
        '.' not in value and
        '-' not in value and
        '{' not in value and
        '}' not in value and
        '%' not in value
    )


def reformat(partition):
    new_df = deque()
    columns = schema.copy()
    for row in partition:
        value = row.value
        if '"' in value:
            text = value.split('"')
            column = text[0][1:].replace(' ', '')
            if column in schema.keys():
                record = text[1]
                if column in ('WhiteElo', 'BlackElo'):
                    try:
                        record = int(record)
                    except ValueError:
                        pass
                elif column == 'Event':
                    record = record.split(' ')[1]
                elif column == 'Site':
                    record = record.split('/')[-1]
                columns[column] = record
        if '1. ' in value:
            notations = [
                ply
                for ply in value.split(' ')
                if is_ply(ply)
            ]
            columns['Notations'] = notations
            new_df.append(list(columns.values()))
            columns = schema.copy()
    return iter(new_df)


@timer
def transform(data: DataFrame):
    df = data.\
        rdd.\
        mapPartitions(reformat).\
        toDF(list(schema.keys()))

    return df

