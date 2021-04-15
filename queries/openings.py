from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    monotonically_increasing_id,
    desc,
    udf,
)


def set_rating_range(elo):
    elo = int(elo)
    rating_range = '< 1200' if elo < 1200 else \
        '1200-1400' if 1200 <= elo <= 1400 else \
        '1400-1600' if 1400 <= elo <= 1600 else \
        '1600-1800' if 1600 <= elo <= 1800 else \
        '1800-2000' if 1800 <= elo <= 2000 else \
        '2000-2200' if 2000 <= elo <= 2200 else \
        '2200-2300' if 2200 <= elo <= 2300 else \
        '2300-2400' if 2300 <= elo <= 2400 else \
        '2400-2500' if 2400 <= elo <= 2500 else \
        '2500-2700' if 2500 <= elo <= 2700 else \
        '2700+' if elo >= 2700 else ''
    return rating_range


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
                regexp_extract(
                    col('value'),
                    f'\\[{title} "(.*?)"]',
                    1
                ),
            ).\
            withColumn(
                'game_id',
                monotonically_increasing_id(),
            ).\
            select(
                col('game_id'),
                col(header),
            ).\
            filter(
                col(header) != '',
            )
        views.append(view)

    rating_range = udf(set_rating_range)
    combined = views[0].join(views[1], ['game_id']).join(views[2], ['game_id'])
    combined = combined.\
        withColumn(
            'rating_range',
            rating_range(combined.white_elo)
        ).\
        select(
            col('opening'),
            col('rating_range')
        )
    combined.show(10, truncate=False)

    spark.stop()


if __name__ == '__main__':
    main()
