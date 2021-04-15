from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecos import get_opening_player


def set_elo_range(opening, white_elo, black_elo):
    opening_player = get_opening_player(opening)
    elo = white_elo if opening_player == 'white' else black_elo
    elo = int(elo)
    elo_range = 'n/a'
    if elo < 1200:
        elo_range = 'below_1200'
    elif 1200 <= elo <= 1400:
        elo_range = '1200_1400'
    elif 1400 <= elo <= 1600:
        elo_range = '1400_1600'
    elif 1600 <= elo <= 1800:
        elo_range = '1600_1800'
    elif 1800 <= elo <= 2000:
        elo_range = '1800_2000'
    elif 2000 <= elo <= 2200:
        elo_range = '2000_2200'
    elif 2200 <= elo <= 2300:
        elo_range = '2200_2300'
    elif 2300 <= elo <= 2400:
        elo_range = '2300_2400'
    elif 2400 <= elo <= 2500:
        elo_range = '2400_2500'
    elif 2500 <= elo <= 2700:
        elo_range = '2500_2700'
    elif elo >= 2700:
        elo_range = 'above_2700'

    return elo_range


def main():
    spark = SparkSession.builder.appName('openings').getOrCreate()
    df = spark.read.text('datasets/jan2013.pgn')

    headers = {
        'opening': 'Opening',
        'white_elo': 'WhiteElo',
        'black_elo': 'BlackElo',
    }
    views = []

    for header, title in headers.items():
        view = df. \
            withColumn(
                header,
                F.regexp_extract(
                    F.col('value'),
                    f'\\[{title} "(.*?)"]',
                    1
                ),
            ).\
            withColumn(
                'game_id',
                F.monotonically_increasing_id(),
            ).\
            select(
                F.col('game_id'),
                F.col(header),
            ).\
            filter(
                (F.col(header) != '') & (F.col(header) != '?'),
            )
        views.append(view)

    elo_range = F.udf(set_elo_range)

    combined = views[0].join(views[1], ['game_id']).join(views[2], ['game_id'])
    combined = combined.\
        withColumn(
            'elo_range',
            elo_range(
                F.col('opening'),
                F.col('white_elo'),
                F.col('black_elo'),
            )
        ).\
        select(
            F.col('game_id'),
            F.col('opening'),
            F.col('elo_range'),
        ).\
        orderBy(F.col('game_id'))
    combined.show(truncate=False)

    spark.stop()


if __name__ == '__main__':
    main()
