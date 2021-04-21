from timeit import default_timer as timer
from datetime import timedelta
from collections import deque

from pyspark.sql.dataframe import DataFrame


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
    'Notations': '',
    'Plies': 0,
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
            columns['Notations'] = value

            plies = 0
            for ply in value.split(' '):
                if is_ply(ply):
                    plies += 1

            columns['Plies'] = plies

            new_df.append(list(columns.values()))
            columns = schema.copy()
    return iter(new_df)


def transform(data: DataFrame):
    start = timer()
    df = data.\
        rdd.\
        mapPartitions(reformat).\
        toDF(list(schema.keys()))
    print(f'Transform: {timedelta(seconds=timer() - start)}')

    return df


def show_or_save(df, query, mode):
    start = timer()
    if mode.title() == 'Show':
        df.show(truncate=False)
    else:
        df.coalesce(8).write.mode('overwrite').parquet(f'savings/{query}')
    print(f'{mode}: {timedelta(seconds=timer() - start)}')
