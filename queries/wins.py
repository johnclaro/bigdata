from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, desc


def main():
    spark = SparkSession.builder.appName('chess').getOrCreate()
    df = spark.read.text('datasets/jan2013.pgn')
    data = df. \
        withColumn(
            'player',
            regexp_extract(
                col('value'),
                f'\\[Result "(.*?)"]',
                1
            ),
        ). \
        filter(
            col('player') != '',
        ). \
        groupBy('player'). \
        count().withColumnRenamed('count', 'wins').\
        sort(
            desc('wins')
        ).\
        replace('1-0', 'white').\
        replace('0-1', 'black').\
        replace('1/2-1/2', 'draw').\
        replace('*', 'n/a')

    data.\
        repartition(1).\
        write.\
        mode('overwrite').\
        csv('datasets/wins', header='true')

    spark.stop()


if __name__ == '__main__':
    main()
