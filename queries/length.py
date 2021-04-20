from timeit import default_timer as timer

from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from helpers import transform, show_or_save


def extract(df: DataFrame):
    start = timer()
    df = df.\
        filter(
            (f.col('BlackElo').isNotNull()) &
            (f.col('WhiteElo').isNotNull())
        )
    return df


def main():
    query = 'survival'
    spark = SparkSession.builder.appName(query).getOrCreate()
    data = spark.read.text('datasets/test.pgn')
    df = transform(data)
    df = extract(df)
    show_or_save(df, query, 'save')
    spark.stop()


if __name__ == '__main__':
    main()
