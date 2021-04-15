from pyspark.sql import SparkSession
from pyspark.sql import functions as F


from ecos import ecos


def set_player(opening):
    try:
        player = ecos[opening]
    except KeyError:
        player = 'white'
    return player


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

    # player = udf(set_player)

    combined = views[0].join(views[1], ['game_id']).join(views[2], ['game_id'])

    combined.show(truncate=False)

    spark.stop()


if __name__ == '__main__':
    main()
