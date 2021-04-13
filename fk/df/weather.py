from pyspark.sql import SparkSession
from pyspark.sql.functions import round, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)


def main():
    spark = SparkSession.builder.appName('MinTemp').getOrCreate()
    schema = StructType([
        StructField('station_id', StringType(), True),
        StructField('date', IntegerType(), True),
        StructField('measure_type', StringType(), True),
        StructField('temperature', FloatType(), True)
    ])
    df = spark.read.schema(schema).csv('fk/files/weather.csv')
    query = df.\
        filter(df.measure_type == 'TMIN').\
        select('station_id', 'temperature').\
        groupBy('station_id').\
        min('temperature').\
        withColumn(
            'temperature',
            round(col('min(temperature)') * 0.1 * (9.0 / 5.0) + 32.0, 2)
        ).\
        select('station_id', 'temperature').\
        sort('temperature')
    for row in query.collect():
        station_id, temperature = row
        print(f'{station_id} \t{temperature:.2f}F')

    spark.stop()


if __name__ == '__main__':
    main()
