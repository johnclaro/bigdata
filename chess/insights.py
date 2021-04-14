from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, regexp_extract, regexp_replace


def main():
    spark = SparkSession.builder.appName('Chess').getOrCreate()
    r1 = spark.read.text('chess/files/july2016.pgn').\
        withColumn(
            'thing',
            regexp_replace(
                split(
                    col('value'),
                    ' '
                )[0],
                '\\[',
                '',
            )
        ). \
        withColumn(
            'data',
            regexp_extract(
                col('value'),
                '\\"(.*?)"',
                1
            )
        ). \
        select(
            col('thing'),
            col('data'),
        )
    r1.filter(
        (col('thing') != '1.') &
        (col('thing') != ''),
    ).show()
    spark.stop()


if __name__ == '__main__':
    main()
