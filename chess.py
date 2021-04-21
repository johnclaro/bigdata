import argparse
from timeit import default_timer as timer
from datetime import timedelta

from pyspark.sql import SparkSession

from apps import openings, plies
from helpers.transformer import transform
from helpers.saver import save


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('app', nargs='+', help='Name of app')
    args = parser.parse_args()
    app = args.app

    spark = SparkSession.builder.appName(app).getOrCreate()
    data = spark.read.text('datasets/test.pgn')
    df = transform(data)

    start = timer()
    if app == 'opening':
        df = openings.extract(df)
    elif app == 'plies':
        df = plies.extract(df)
    print(f'Extracting: {timedelta(seconds=timer() - start)}')

    df.show(truncate=False)
    # save(df, app)
    spark.stop()


if __name__ == '__main__':
    main()
