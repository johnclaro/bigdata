from timeit import default_timer as timer
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def set_elo_range(white_elo: f.col, black_elo: f.col) -> str:
    white_elo, black_elo = int(white_elo), int(black_elo)
    elo = (white_elo + black_elo) / 2
    elo_range = 'n/a'
    if elo < 1200:
        elo_range = '<1200'
    elif 1200 <= elo <= 1400:
        elo_range = '1200-1400'
    elif 1400 <= elo <= 1600:
        elo_range = '1400-1600'
    elif 1600 <= elo <= 1800:
        elo_range = '1600-1800'
    elif 1800 <= elo <= 2000:
        elo_range = '1800-2000'
    elif 2000 <= elo <= 2200:
        elo_range = '2000-2200'
    elif 2200 <= elo <= 2300:
        elo_range = '2200-2300'
    elif 2300 <= elo <= 2400:
        elo_range = '2300-2400'
    elif 2400 <= elo <= 2500:
        elo_range = '2400-2500'
    elif 2500 <= elo <= 2700:
        elo_range = '2500-2700'
    elif 2700 <= elo:
        elo_range = '2700+'

    return elo_range


def main():
    start = timer()
    spark = SparkSession.builder.appName('openings').getOrCreate()
    data = spark.read.text('datasets/jan2013.pgn')

    headers = {
        'opening': 'Opening',
        'white_elo': 'WhiteElo',
        'black_elo': 'BlackElo',
    }
    views = []

    for header, title in headers.items():
        view = data. \
            withColumn(
                header,
                f.regexp_extract(
                    f.col('value'),
                    f'\\[{title} "(.*?)"]',
                    1
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
            filter(
                (f.col(header) != '') & (f.col(header) != '?'),
            )
        views.append(view)

    set_elo_range_udf = f.udf(set_elo_range)
    df = views[0].join(views[1], ['game_id']).join(views[2], ['game_id'])
    openings_df = df.\
        groupBy(
            f.col('opening')
        ).\
        count().\
        orderBy(
            f.desc(
                f.col('count')
            )
        ).\
        select(
            f.col('opening')
        ).\
        limit(10)

    openings = [
        opening[0]
        for opening in openings_df.collect()
    ]

    df = df.\
        withColumn(
            'elo_range',
            set_elo_range_udf(
                f.col('white_elo'),
                f.col('black_elo'),
            )
        ).\
        select(
            f.col('game_id'),
            f.col('opening'),
            f.col('elo_range'),
        ).\
        filter(
            f.col('opening').isin(openings)
        ).\
        groupBy(
            f.col('opening'),
        ).\
        pivot('elo_range').\
        count().\
        na.\
        fill(0)

    print('-------------------------------------')
    print(f'{timedelta(seconds=timer() - start)}')
    print('-------------------------------------')
    df.show(10, truncate=False)
    # df. \
    #     repartition(1). \
    #     write. \
    #     mode('overwrite'). \
    #     csv('datasets/openings', header='true')

    spark.stop()


if __name__ == '__main__':
    main()
