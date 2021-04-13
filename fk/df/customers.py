from pyspark.sql import SparkSession
from pyspark.sql.functions import round, sum, desc
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
)


def main():
    spark = SparkSession.builder.appName('Customers').getOrCreate()
    schema = StructType([
        StructField('customer_id', IntegerType(), True),
        StructField('item_id', IntegerType(), True),
        StructField('spent', FloatType(), True),
    ])
    spark.read.schema(schema).csv('fk/files/customers.csv').\
        groupBy('customer_id').\
        agg(
            round(sum('spent'), 2).alias('spent')
        ).\
        sort(desc('spent')).\
        show()
    spark.stop()


if __name__ == '__main__':
    # TODO: Add up amount spent by customer
    # Load customers.csv as DF
    # Group by customer_id
    # Sum by spent (round to 2 decimal places) -> agg(round(sum(), 2)
    # Sort by total spent
    # Show results
    main()
