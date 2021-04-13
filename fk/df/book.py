from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, desc


def main():
    spark = SparkSession.builder.appName('WordCount').getOrCreate()
    df = spark.read.text('fk/files/book.txt')

    # Split the line of text into list of words then use explode to create
    # multiple rows for those list of words
    words = df.\
        select(
            explode(split(df.value, '\\W+')).alias('word')
        )
    query = words. \
        filter(words.word != '').\
        select(
            lower(words.word).alias('word')
        ).\
        groupBy('word').\
        count().\
        sort(desc('count'))
    query.show()
    spark.stop()


if __name__ == '__main__':
    main()
