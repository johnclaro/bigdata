from timeit import default_timer as timer
from datetime import timedelta

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
    '<1200',
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
    headers = {
        'opening': {
            'title': 'Opening',
            'data_type': StringType,
        },
        'white_elo': {
            'title': 'WhiteElo',
            'data_type': IntegerType,
        },
        'black_elo': {
            'title': 'BlackElo',
            'data_type': IntegerType,
        },
    }
    views = []

    for header, info in headers.items():
        filters = ()
        if info['data_type'] == IntegerType:
            filters = (f.col(header).isNotNull())
        elif info['data_type'] == StringType:
            filters = (f.col(header) != '')

        view = data. \
            withColumn(
                header,
                f.regexp_extract(
                    f.col('value'),
                    f'\\[{info["title"]} "(.*?)"]',
                    1
                ).cast(
                    info['data_type']()
                ),
            ).\
            withColumn(
                'game_id',
                f.monotonically_increasing_id(),
            ).\
            select(
                f.col('game_id'),
                f.col(header),
            ).\
            filter(filters)
        views.append(view)

    df = views[0].join(views[1], ['game_id']).join(views[2], ['game_id'])
    bucketizer = Bucketizer(
        splits=SPLITS,
        inputCol='white_elo',
        outputCol='elo_range_id',
    )
    df = bucketizer.transform(df)
    label_array = f.array(
        *(f.lit(label) for label in LABELS)
    )
    df = df.\
        withColumn(
            'elo_range',
            label_array.getItem(
                f.col('elo_range_id').cast('integer')
            )
        ).\
        groupBy('opening').\
        pivot('elo_range').\
        count().\
        na.\
        fill(0)

    return df


def main():
    start = timer()
    spark = SparkSession.builder.appName('openings').getOrCreate()
    data = spark.read.text('datasets/jan2013.pgn')
    df = extract(data)
    print('-------------------------------------')
    print(f'{timedelta(seconds=timer() - start)}')
    print('-------------------------------------')
    df.show(5, truncate=False)

    # df. \
    #     repartition(1). \
    #     write. \
    #     mode('overwrite'). \
    #     csv('datasets/openings', header='true')

    spark.stop()


if __name__ == '__main__':
    main()