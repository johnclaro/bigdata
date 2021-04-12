import re

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Book')
sc = SparkContext(conf=conf)


def normalize_words(text: str) -> list:
    words = re.compile(r'\W+', re.UNICODE).split(text.lower())
    return words


def init_occurences(word):
    return (word, 1)


def increment_occurences(total: int, increment: int) -> int:
    """Increments the total number of occurences by the 'increment' value.

    Args:
        total: Total number of times the word has occured
        increment: Value returned by 'init_occurences' method

    Returns:
        Number of times a word has occured
    """
    return total + increment


def switch_position(record: tuple) -> tuple:
    """Flips a record to enable sorting by word."""
    word = record[0]
    count = record[1]
    return (count, word)


text = sc.textFile('fk/datasets/book.txt')
words = text.flatMap(normalize_words)
word_counts = words.map(init_occurences).reduceByKey(increment_occurences)
word_counts_sorted = word_counts.map(switch_position).sortByKey()
results = word_counts_sorted.collect()
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(f'{word}: \t\t{count}')
