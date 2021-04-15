from operator import add
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from helpers import ecos


def get_opening_player(opening: str) -> str:
    try:
        player = ecos[opening]
    except KeyError:
        player = 'white'
    return player


def set_elo_range(opening, white_elo, black_elo):
    opening_player = get_opening_player(opening)
    elo = white_elo if opening_player == 'white' else black_elo
    elo = int(elo)
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
    elif elo >= 2700:
        elo_range = '2700+'

    return elo_range


def main():
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

    elo_range = f.udf(set_elo_range)
    df = views[0].join(views[1], ['game_id']).join(views[2], ['game_id'])
    df = df.\
        withColumn(
            'elo_range',
            elo_range(
                f.col('opening'),
                f.col('white_elo'),
                f.col('black_elo'),
            )
        ).\
        select(
            f.col('game_id'),
            f.col('opening'),
            f.col('elo_range'),
        ).\
        groupBy(
            f.col('opening'),
        ).\
        pivot('elo_range').\
        count().\
        na.fill(0)

    columns = [
        column
        for column in df.columns
        if column not in ('opening', 'elo_range')
    ]

    df = df.\
        withColumn(
            'total',
            reduce(
                add,
                [f.col(column) for column in columns]
            ),
        ).\
        select(
            'opening',
            *columns
        ).\
        orderBy(
            f.desc(
                f.col('total'),
            ),
        )

    df. \
        repartition(1). \
        write. \
        mode('overwrite'). \
        csv('datasets/openings', header='true')

    spark.stop()


if __name__ == '__main__':
    main()
