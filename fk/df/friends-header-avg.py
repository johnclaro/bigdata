from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round


def main():
    spark = SparkSession.builder.appName('SparkSQL').getOrCreate()
    data = spark.\
        read.\
        option('header', 'true').\
        option('inferSchema', 'true').\
        csv('fk/files/friends-header.csv')
    query = data.\
        groupBy('age').\
        agg(
            round(avg('friends'), 2).alias('friends_avg')
        ).\
        sort('age')
    query.show()
    spark.stop()


if __name__ == '__main__':
    main()
