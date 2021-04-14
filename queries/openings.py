from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    monotonically_increasing_id,
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
        view.show()
        views.append(view)

    views[0].join(views[1], ['game_id']).join(views[2], ['game_id']).show()

    spark.stop()


if __name__ == '__main__':
    main()
