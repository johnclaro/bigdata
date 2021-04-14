from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    desc,
)


def main():
    spark = SparkSession.builder.appName('chess').getOrCreate()
    df = spark.read.text('datasets/jan2013.pgn')
    data = df. \
        withColumn(
            'opening',
            regexp_extract(
                col('value'),
                f'\\[Opening "(.*?)"]',
                1
            )
        ). \
        filter(
            (col('opening') != '')
        ). \
        select(
            col('opening')
        ). \
        groupBy('opening'). \
        count(). \
        sort(
            desc('count')
        )
    data.repartition(1).write.csv('files/openings', header='true')

    spark.stop()


if __name__ == '__main__':
    main()
