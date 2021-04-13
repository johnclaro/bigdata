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


def reverse_position(rdd: Tuple[int, float]) -> Tuple[float, int]:
    """Reverses the position of an element.

    Args:
        rdd: RDD of customers

    Returns:
        A tuple with amount spent and customer ID
    """
    return rdd[1], rdd[0]


def main():
    conf = SparkConf().setMaster('local').setAppName('Customers')
    sc = SparkContext(conf=conf)
    data = sc.textFile('fk/files/customers.csv')
    rdd = data.map(parse_data) \
        .map(map_spent) \
        .reduceByKey(add_spent) \
        .map(reverse_position) \
        .sortByKey()
    for element in rdd.collect():
        spent, customer_id = element
        print(f'{customer_id} \t {spent:.2f}')


if __name__ == '__main__':
    main()
