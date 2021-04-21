import os
from timeit import default_timer as timer
from datetime import timedelta
from collections import deque

import pandas as pd
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


def transform(data: DataFrame):
    start = timer()
    df = data.\
        rdd.\
        mapPartitions(reformat).\
        toDF(list(schema.keys()))
    print(f'Transform: {timedelta(seconds=timer() - start)}')

    return df


def save(df, query):
    start = timer()
    df.coalesce(8).write.mode('overwrite').parquet(f'savings/{query}')
    print(f'Saving: {timedelta(seconds=timer() - start)}')

    folder_path = f'savings/{query}'

    dfs = []
    for file in os.listdir(folder_path):
        if file.endswith('parquet'):
            df = pd.read_parquet(f'{folder_path}/{file}')
            dfs.append(df)

    csv_file = f'files/{query}.csv'
    df = pd.concat(dfs)

    if query == 'plies':
        df = df.sort_values(by=['Plies'])

    df.to_csv(csv_file, index=False)
    print(f'Saved {csv_file}')
