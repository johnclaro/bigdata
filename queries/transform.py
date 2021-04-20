from timeit import default_timer as timer
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


schema = StructType([
    StructField('event', StringType(), True),
    StructField('site', StringType(), True),
    StructField('white', StringType(), True),
    StructField('black', StringType(), True),
    StructField('result', StringType(), True),
    StructField('utc_date', StringType(), True),
    StructField('utc_time', StringType(), True),
    StructField('white_elo', IntegerType(), True),
    StructField('black_elo', IntegerType(), True),
    StructField('black_rating_diff', StringType(), True),
    StructField('white_rating_diff', StringType(), True),
    StructField('eco', StringType(), True),
    StructField('opening', StringType(), True),
    StructField('time_control', StringType(), True),
    StructField('termination', StringType(), True),
])


def reformat(df):
    new_df = []
    columns = []
    for row in df:
        value = row.value
        if '"' in value and '[BlackTitle ' not in value and '[WhiteTitle ' not in value:
            text = value.split('"')[1]
            columns.append(text)
        elif '1.' in value:
            columns.append(value)
            new_df.append(columns)
            columns = []
    return iter(new_df)


def transform(df):
    columns = [
        'event',
        'site',
        'white',
        'black',
        'result',
        'utc_date',
        'utc_time',
        'white_elo',
        'black_elo',
        'white_rating_diff',
        'black_rating_diff',
        'eco',
        'opening',
        'time_control',
        'termination',
        'notation'
    ]
    df = df.\
        rdd.\
        mapPartitions(reformat).\
        toDF(columns)

    return df


def main():
    filename = '93mb.pgn'
    start = timer()
    spark = SparkSession.builder.appName('openings').getOrCreate()
    data = spark.read.text(f'datasets/{filename}')
    df = transform(data)

    extract_timer = timer()
    print(f'Extracting {filename}: {timedelta(seconds=timer() - start)}')
    # df.show(20, truncate=False)
    df.coalesce(1).write.mode('overwrite').csv('files/transform')
    print(f'End {filename}: {timedelta(seconds=timer() - extract_timer)}')

    spark.stop()


if __name__ == '__main__':
    main()
