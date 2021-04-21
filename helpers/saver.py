import os
import pandas as pd

from timeit import default_timer as timer
from datetime import timedelta


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