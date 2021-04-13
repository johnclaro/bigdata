from typing import Tuple

from pyspark import SparkConf, SparkContext


def parse_data(data: str) -> Tuple[int, int, float]:
    """Parses CSV data into relevant fields.

    Args:
        data: Rows of a CSV file

    Returns:
        A tuple version of each row
    """
    rows = data.split(',')
    customer_id = int(rows[0])
    item_id = int(rows[1])
    spent = float(rows[2])
    return customer_id, item_id, spent


def map_spent(items: Tuple[int, int, float]) -> Tuple[int, float]:
    """Maps the customer ID and amount spent.

    Args:
        items: RDD of parsed data

    Returns:
        A tuple of customer and amount spent
    """
    return items[0], items[2]


def add_spent(total: float, current: float) -> float:
    """Adds up all the amount spent by customer.

    Args:
        total: Total amount spent by customer
        current: Current amount spent by customer

    Returns:
        Sum of amount spent by a customer
    """
    return total + current


def main():
    conf = SparkConf().setMaster('local').setAppName('Customers')
    sc = SparkContext(conf=conf)
    customers_rdd = sc.textFile('fk/datasets/customers.csv')
    customers_rdd = customers_rdd.map(parse_data)
    customers_rdd = customers_rdd.map(map_spent)
    customers_rdd = customers_rdd.reduceByKey(add_spent)
    for customer in customers_rdd.collect():
        print(f'{customer[0]} \t {customer[1]:.2f}')


if __name__ == '__main__':
    main()
