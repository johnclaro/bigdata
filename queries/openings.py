from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    monotonically_increasing_id,
    count,
    udf,
    desc,
)


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

    elo_below_1200 = udf(lambda elo: '1' if int(elo) < 1200 else '0')
    elo_1200_1400 = udf(lambda elo: '1' if 1200 <= int(elo) < 1400 else '0')
    elo_1400_1600 = udf(lambda elo: '1' if 1400 <= int(elo) < 1600 else '0')

    combined = views[0].join(views[1], ['game_id']).join(views[2], ['game_id'])
    combined = combined.\
        withColumn(
            '<1200',
            elo_below_1200(combined.white_elo)
        ).\
        withColumn(
            '1200-1400',
            elo_1200_1400(combined.white_elo)
        ).\
        withColumn(
            '1400-1600',
            elo_1400_1600(combined.white_elo)
        ).\
        select(
            col('opening'),
            col('<1200'),
            col('1200-1400'),
            col('1400-1600'),
        )

    combined.show(truncate=False)

    spark.stop()


if __name__ == '__main__':
    main()
