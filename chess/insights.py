from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    split,
    col,
    regexp_extract,
    regexp_replace,
    collect_list,
)


def main():
    spark = SparkSession.builder.appName('Chess').getOrCreate()
    spark.read.text('chess/files/test.pgn').\
        withColumn(
            'event',
            regexp_extract(
                col('value'),
                '\\[Event "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'site',
            regexp_extract(
                col('value'),
                '\\[Site "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'white',
            regexp_extract(
                col('value'),
                '\\[White "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'black',
            regexp_extract(
                col('value'),
                '\\[Black "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'utc_date',
            regexp_extract(
                col('value'),
                '\\[UTCDate "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'utc_time',
            regexp_extract(
                col('value'),
                '\\[UTCTime "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'white_elo',
            regexp_extract(
                col('value'),
                '\\[WhiteElo "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'black_elo',
            regexp_extract(
                col('value'),
                '\\[BlackElo "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'white_rating_diff',
            regexp_extract(
                col('value'),
                '\\[WhiteRatingDiff "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'black_rating_diff',
            regexp_extract(
                col('value'),
                '\\[BlackRatingDiff "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'eco',
            regexp_extract(
                col('value'),
                '\\[ECO "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'opening',
            regexp_extract(
                col('value'),
                '\\[Opening "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'time_control',
            regexp_extract(
                col('value'),
                '\\[TimeControl "(.*?)"]',
                1
            )
        ). \
        withColumn(
            'termination',
            regexp_extract(
                col('value'),
                '\\[Termination "(.*?)"]',
                1
            )
        ). \
        select(
            col('event'),
            col('site'),
            col('white'),
            col('black'),
            col('utc_date'),
            col('utc_time'),
            col('white_elo'),
            col('black_elo'),
            col('white_rating_diff'),
            col('black_rating_diff'),
            col('eco'),
            col('opening'),
            col('time_control'),
            col('termination'),
        ).show()
    spark.stop()


if __name__ == '__main__':
    main()
