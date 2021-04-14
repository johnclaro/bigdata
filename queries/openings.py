from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    monotonically_increasing_id,
)


def main():
    spark = SparkSession.builder.appName('openings').getOrCreate()
    df = spark.read.text('datasets/jan2013.pgn')
    df. \
        withColumn(
            'opening',
            regexp_extract(
                col('value'),
                '\\[Opening "(.*?)"]',
                1
            ),
        ).\
        withColumn(
            'game_id',
            monotonically_increasing_id(),
        ).\
        select(
            col('game_id'),
            col('opening'),
        ).\
        filter(
            col('opening') != '',
        ).\
        show()

    spark.stop()


if __name__ == '__main__':
    main()
