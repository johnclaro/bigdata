from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    desc,
)
from pyspark.sql.dataframe import DataFrame


def top_100_openings(df: DataFrame):
    data = df.\
        withColumn(
            'opening',
            regexp_extract(
                col('value'),
                f'\\[Opening "(.*?)"]',
                1
            )
        ).\
        filter(
            (col('opening') != '')
        ).\
        select(
            col('opening')
        ).\
        groupBy('opening').\
        count().\
        sort(
            desc('count')
        )
    data.write.csv('chess/files/top_openings_100.csv')


def main():
    spark = SparkSession.builder.appName('chess').getOrCreate()
    df = spark.read.text('chess/files/jan2013.pgn')
    top_100_openings(df)

    spark.stop()


if __name__ == '__main__':
    main()
