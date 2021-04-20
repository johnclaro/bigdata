from timeit import default_timer as timer
from datetime import timedelta

from pyspark.sql import SparkSession

from helpers import transform


def main():
    filename = '93mb.pgn'
    start = timer()
    spark = SparkSession.builder.appName('openings').getOrCreate()
    data = spark.read.text(f'datasets/{filename}')
    df = transform(data)

    extract_timer = timer()
    print(f'Extracting {filename}: {timedelta(seconds=timer() - start)}')
    # df.show(20, truncate=False)
    df.coalesce(8).write.mode('overwrite').parquet(f'files/transform/{filename}')
    print(f'End {filename}: {timedelta(seconds=timer() - extract_timer)}')

    spark.stop()


if __name__ == '__main__':
    main()
