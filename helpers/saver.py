import os
import pandas as pd

from helpers.timer import timer


@timer
def show_or_save(df, app, save):
    if save:
        partitions = f'partitions/{app}'
        df.coalesce(8).write.mode('overwrite').parquet(partitions)

        dfs = []
        for file in os.listdir(partitions):
            if file.endswith('parquet'):
                df = pd.read_parquet(f'{partitions}/{file}')
                dfs.append(df)

        csv_file = f'csvs/{app}.csv'
        df = pd.concat(dfs)

        if app == 'plies':
            df = df.sort_values(by=['Plies'])
        elif app == 'moves':
            df = df.sort_values(by=['Count'], ascending=False)

        df.to_csv(csv_file, index=False)
    else:
        df.show(truncate=False)
