import os

import pandas as pd


def main():
    schema = 'transform/1gb.pgn'
    folder_path = f'files/{schema}'

    dfs = []
    for file in os.listdir(folder_path):
        if file.endswith('parquet'):
            df = pd.read_parquet(f'{folder_path}/{file}')
            dfs.append(df)

    csv_file = f'files/{schema}.csv'
    df = pd.concat(dfs)

    if schema == 'openings':
        df = df.sort_values(by='total', ascending=False)

    df.to_csv(csv_file, index=False)
    print(f'Saved {csv_file}')


if __name__ == '__main__':
    main()
