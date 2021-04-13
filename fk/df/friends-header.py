from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName('SparkSQL').getOrCreate()
    people = spark.\
        read.\
        option('header', 'true').\
        option('inferSchema', 'true').\
        csv('fk/datasets/friends-header.csv')

    print('Here is our schema')
    people.printSchema()

    print("Let's display the name column")
    people.select('name').show()

    print('Filter out anyone over 21')
    people.filter(people.age < 21).show()

    print('Group by age')
    people.groupBy('age').count().show()

    print('Make everyone 10 years older')
    people.select(people.name, people.age + 10).show()

    spark.stop()


if __name__ == '__main__':
    main()
