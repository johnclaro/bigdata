import os
import pandas as pd

from timeit import default_timer as timer
from datetime import timedelta


def save(df, app):
    start = timer()
    df.coalesce(8).write.mode('overwrite').parquet(f'savings/{app}')
    print(f'Saving: {timedelta(seconds=timer() - start)}')

    folder_path = f'savings/{app}'

    dfs = []
    for file in os.listdir(folder_path):
        if file.endswith('parquet'):
            df = pd.read_parquet(f'{folder_path}/{file}')
            dfs.append(df)

    csv_file = f'files/{app}.csv'
    df = pd.concat(dfs)

    if app == 'plies':
        df = df.sort_values(by=['Plies'])

    df.to_csv(csv_file, index=False)
    print(f'Saved {csv_file}')
