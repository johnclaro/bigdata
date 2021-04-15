import os

import pandas as pd


def main():
    schema = 'openings'
    folder_path = f'datasets/{schema}'

    dfs = []
    for file in os.listdir(folder_path):
        if file.endswith('parquet'):
            df = pd.read_parquet(f'{folder_path}/{file}')
            dfs.append(df)

    csv_file = f'files/{schema}.csv'
    merged = pd.concat(dfs)
    merged.to_csv(csv_file, index=False)
    print(f'Saved {csv_file}')


if __name__ == '__main__':
    main()
