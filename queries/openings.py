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


def main():
    start = timer()
    spark = SparkSession.builder.appName('openings').getOrCreate()
    data = spark.read.text('datasets/43gb.pgn')
    df = extract(data)

    print('-------------------------------------')
    extract_timer = timer()
    print(f'Extracting took {timedelta(seconds=extract_timer - start)}')
    print('-------------------------------------')

    # df.show(10, truncate=False)
    df.repartition(4).write.mode('overwrite').parquet('datasets/openings')
    print('-------------------------------------')
    print(f'Saving / showing took {timedelta(seconds=timer() - extract_timer)}')
    print('-------------------------------------')

    spark.stop()


if __name__ == '__main__':
    main()
