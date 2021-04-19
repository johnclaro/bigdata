import os
from shutil import copyfile
from timeit import default_timer as timer
from datetime import timedelta
from functools import reduce
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.ml.feature import Bucketizer
from pyspark.sql.types import IntegerType, StringType

SPLITS = [
    0,
    1200,
    1400,
    1600,
    1800,
    2000,
    2200,
    2300,
    2400,
    2500,
    2700,
    9999,
]

LABELS = (
    '0-1200',
    '1200-1400',
    '1400-1600',
    '1600-1800',
    '1800-2000',
    '2000-2200',
    '2200-2300',
    '2300-2400',
    '2400-2500',
    '2500-2700',
    '2700+',
)


def extract(data):
    schema = {
        'opening': {
            'name': 'Opening',
            'data_type': StringType,
        },
        'white_elo': {
            'name': 'WhiteElo',
            'data_type': IntegerType,
        },
        'black_elo': {
            'name': 'BlackElo',
            'data_type': IntegerType,
        },
    }
    views = []

    for column, field in schema.items():
        filters = ()
        if field['data_type'] == IntegerType:
            filters = (f.col(column).isNotNull())
        elif field['data_type'] == StringType:
            filters = (f.col(column) != '')

        view = data. \
            withColumn(
                column,
                f.regexp_extract(
                    f.col('value'),
                    f'\\[{field["name"]} "(.*?)"]',
                    1
                ).cast(
                    field['data_type'](),
                ),
            ).\
            withColumn(
                'game_id',
                f.monotonically_increasing_id(),
            ).\
            select(
                f.col('game_id'),
                f.col(column),
            ).\
            filter(filters)
        views.append(view)

    df = views[0].join(views[1], ['game_id']).join(views[2], ['game_id'])

    average_func = sum(x for x in [f.col('white_elo'), f.col('black_elo')]) / 2
    df = df.\
        withColumn(
            'elo',
            average_func
        )

    buckets = Bucketizer(
        splits=SPLITS,
        inputCol='elo',
        outputCol='elo_range_id',
    )
    df = buckets.transform(df)
    labels = f.array(
        *(f.lit(label) for label in LABELS)
    )
    df = df.\
        withColumn(
            'elo_range',
            labels.getItem(
                f.col('elo_range_id').cast('integer')
            )
        ).\
        groupBy('opening').\
        pivot('elo_range').\
        count().\
        na.\
        fill(0)

    df = df. \
        withColumn(
            'total',
            reduce(
                add,
                [f.col(x) for x in df.columns[1:]],
            )
        ). \
        orderBy(
            f.desc(
                f.col('total')
            )
        ). \
        limit(10)

    return df


def main(dataset: str):
    start = timer()
    spark = SparkSession.builder.appName('openings').getOrCreate()
    data = spark.read.text(dataset)
    df = extract(data)

    extract_timer = timer()
    print(f'Extracting {dataset} took {timedelta(seconds=timer() - start)}')
    # df.show(10, truncate=False)
    df.repartition(4).write.mode('overwrite').parquet(f'files/{dataset}')
    print(f'End of {dataset} took {timedelta(seconds=timer() - extract_timer)}')

    spark.stop()


if __name__ == '__main__':
    filepaths = (
        # 'datasets/lichess_db_standard_rated_2017-01.pgn',
        # 'datasets/lichess_db_standard_rated_2017-02.pgn',
        # '/Volumes/USB/lichess_db_standard_rated_2017-03.pgn',
        # '/Volumes/USB/lichess_db_standard_rated_2017-04.pgn',
        # '/Volumes/USB/lichess_db_standard_rated_2017-05.pgn',
        # '/Volumes/USB/lichess_db_standard_rated_2017-06.pgn',
        # '/Volumes/USB/lichess_db_standard_rated_2017-07.pgn',
    )
    for filepath in filepaths:
        new_filepath = ''
        if 'Volumes' in filepath:
            filename = filepath.split('/')[-1]
            new_filepath = f'datasets/{filename}'
            copyfile(filepath, new_filepath)

        if new_filepath:
            main(new_filepath)
            os.remove(new_filepath)
        else:
            main(filepath)
