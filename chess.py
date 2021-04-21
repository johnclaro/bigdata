import argparse
from pyspark.sql import SparkSession

from apps import openings, plies
from helpers.transformer import transform
from helpers.saver import show_or_save


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'app',
        nargs=1,
        choices=('openings', 'plies'),
        help='Name of app'
    )
    parser.add_argument(
        '--save',
        dest='save',
        action='store_true'
    )
    parser.set_defaults(save=False)
    args = parser.parse_args()
    app = args.app[0]
    save = args.save

    data_file = 'datasets/test.pgn'

    print('============================')
    print(f'{app.title()} - {data_file}')
    print('============================')
    spark = SparkSession.builder.appName(app).getOrCreate()
    data = spark.read.text(data_file)
    df = transform(data)

    if app == 'openings':
        df = openings.extract(df)
    elif app == 'plies':
        df = plies.extract(df)

    show_or_save(df, app, save=save)
    spark.stop()


if __name__ == '__main__':
    main()
