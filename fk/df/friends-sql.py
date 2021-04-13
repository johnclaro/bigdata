from typing import Tuple

from pyspark.sql import Row, SparkSession


def parse_csv(data: str) -> Tuple[int, str, int, int]:
    """Parses CSV file to initialize a PySpark SQL Row."""
    rows = data.split(',')
    person_id = int(rows[0])
    name = str(rows[1])
    age = int(rows[2])
    num_friends = int(rows[3])
    return Row(ID=person_id, name=name, age=age, num_friends=num_friends)


def main():
    spark = SparkSession.builder.appName('SparkSQL').getOrCreate()
    data = spark.sparkContext.textFile('fk/files/friends.csv')
    people = data.map(parse_csv)

    # Register DataFrame as a table
    schema = spark.createDataFrame(people).cache()
    schema.createOrReplaceTempView('people')

    # SQL can be used over DataFrames registered as a table
    teenagers = spark.sql('SELECT * FROM people WHERE age >= 13 AND age <= 19')
    for teenager in teenagers.collect():
        print(teenager)

    # Functions can also be used instead of SQL queries
    schema.groupBy('age').count().orderBy('age').show()
    spark.stop()


if __name__ == '__main__':
    main()
