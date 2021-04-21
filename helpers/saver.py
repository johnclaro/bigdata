import os
import pandas as pd

from timeit import default_timer as timer
from datetime import timedelta


def save(df, app):
    start = timer()
    partitions = f'partitions/{app}'
    df.coalesce(8).write.mode('overwrite').parquet(partitions)
    print(f'Saving: {timedelta(seconds=timer() - start)}')

    dfs = []
    for file in os.listdir(partitions):
        if file.endswith('parquet'):
            df = pd.read_parquet(f'{partitions}/{file}')
            dfs.append(df)

    csv_file = f'output/{app}.csv'
    df = pd.concat(dfs)

    if app == 'plies':
        df = df.sort_values(by=['Plies'])

    df.to_csv(csv_file, index=False)
    print(f'Saved {csv_file}')
