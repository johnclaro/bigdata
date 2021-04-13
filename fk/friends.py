from typing import Tuple

from pyspark import SparkConf, SparkContext


def parse_lines(line: str) -> Tuple[int, int]:
    fields = line.split(',')
    age = int(fields[2])
    friends = int(fields[3])
    return age, friends


def init_friends(friends: int) -> tuple:
    """Initialises the number of friends for an age.

    Args:
        friends: Number of friends of an age.

    Returns:
        A tuple that becomes the value for the key 'age'.
            0 - Number of friends of an age.
            1 - Number of times this age has occured during aggregation.
    """
    return friends, 1


def increment_friends(x: Tuple[int, int],
                      y: Tuple[int, int]) -> Tuple[int, int]:
    """Gathers all values for each age and adds each of them together.

    Args:
        x: Value of an age.
        y: Value of an age.

    Returns:
        A tuple that becomes the value for the key 'age'.
            0 - Total number of friends of an age.
            1 - Total number of times age occurs during aggregation.
    """
    friends = x[0] + y[0]
    occurrences = x[1] + y[1]
    return friends, occurrences


def calculate_average(x: tuple) -> float:
    """"Calculates average number of friends for each age."""
    friends = x[0]
    occurrences = x[1]
    average = friends / occurrences
    return average


def main():
    conf = SparkConf().setMaster('local').setAppName('FriendsByAge')
    sc = SparkContext(conf=conf)
    data = sc.textFile('fk/datasets/friends.csv')
    friends = data.map(parse_lines)\
        .mapValues(init_friends)\
        .reduceByKey(increment_friends)\
        .mapValues(calculate_average)
    for element in friends.collect():
        print(element)


if __name__ == '__main__':
    main()
