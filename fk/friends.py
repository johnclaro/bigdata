from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('FriendsByAge')
context = SparkContext(conf=conf)


def parse_lines(line: str) -> tuple:
    fields = line.split(',')
    age = int(fields[2])
    friends = int(fields[3])
    return (age, friends)


def init_friends(friends: int) -> tuple:
    """Initialises the number of friends for an age.

    Args:
        friends: Number of friends of an age.

    Returns:
        A tuple that becomes the value for the key 'age'.
            0 - Number of friends of an age.
            1 - Number of times this age has occured during aggregation.
    """
    return (friends, 1)


def increment_friends(x: tuple, y: tuple) -> tuple:
    """Gathers all values for each age and adds each of them together.

    Args:
        x: Value of an age.
        y: Value of an age.

    Returns:
        A tuple that becomes the value for the key 'age'.
            0 - Total number of friends of an age.
            1 - Total number of times age occured during aggregation.
    """
    friends = x[0] + y[0]
    occurences = x[1] + y[1]
    return (friends, occurences)


def calculate_average(x: tuple) -> float:
    """"Calculates average number of friends for each age."""
    friends = x[0]
    occurences = x[1]
    average = friends / occurences
    return average


lines = context.textFile('fk/datasets/friends.csv')
rdd = lines.map(parse_lines)
totals = rdd.mapValues(init_friends).reduceByKey(increment_friends)
averages = totals.mapValues(calculate_average)
results = averages.collect()
for result in results:
    print(result)
