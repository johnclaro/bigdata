from timeit import default_timer as timer
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, desc


def extract(data):
    df = data. \
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

    return df


def main():
    start = timer()
    spark = SparkSession.builder.appName('wins').getOrCreate()
    data = spark.read.text('datasets/43gb.pgn')
    df = extract(data)
    df.repartition(4).write.mode('overwrite').parquet('datasets/wins')

    print('-------------------------------------')
    extract_timer = timer()
    print(f'Extracting took {timedelta(seconds=extract_timer - start)}')
    print('-------------------------------------')

    # df.show(truncate=False)
    df.repartition(4).write.mode('overwrite').parquet('datasets/wins')
    print('-------------------------------------')
    print(f'Saving / showing took {timedelta(seconds=timer() - extract_timer)}')
    print('-------------------------------------')

    spark.stop()


if __name__ == '__main__':
    main()
