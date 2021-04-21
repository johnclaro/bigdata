import os

import pandas as pd


def main():
    query = 'plies'
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


if __name__ == '__main__':
    main()
