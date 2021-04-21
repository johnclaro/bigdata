import argparse
import os
from pyspark.sql import SparkSession

from apps import openings, plies
from helpers.transformer import transform
from helpers.saver import show_or_save


def main():
    choices = [
        filename.replace('.py', '')
        for filename in os.listdir('apps')
        if '__' not in filename
    ]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'app',
        nargs=1,
        choices=choices,
        help='Name of app',
    )
    parser.add_argument(
        '-s',
        '--save',
        dest='save',
        action='store_true',
        help='Saves output to CSV otherwise just prints it',
    )
    parser.add_argument(
        '-f',
        '--file',
        required=True,
        dest='filename',
        help='Name of file in datasets folder',
    )
    parser.set_defaults(save=False)
    args = parser.parse_args()
    app = args.app[0]
    save = args.save
    filename = args.filename
    filepath = f'datasets/{filename}.pgn'

    print('-------------------------------------------------------------------')
    print(f'function \t time \t\t {app} \t {filepath}')
    print('-------------------------------------------------------------------')
    spark = SparkSession.builder.appName(app).getOrCreate()
    data = spark.read.text(filepath)
    df = transform(data)

    if app == 'openings':
        df = openings.extract(df)
    elif app == 'plies':
        df = plies.extract(df)

    show_or_save(df, app, save=save)
    spark.stop()


if __name__ == '__main__':
    main()
