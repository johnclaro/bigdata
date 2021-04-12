import re
from typing import Tuple

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Book')
sc = SparkContext(conf=conf)


def normalize_words(text: str) -> list:
    """Uses regular expression to search for proper words.

    Args:
        text: The whole book in txt format

    Returns:
        A list of lowercased words
    """
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


def map_words(word: str) -> Tuple[str, int]:
    """Maps the word with an increment value of 1.

    Args:
        word: A word in the book

    Returns:
        A tuple containing the word and an increment value of 1
    """
    return word, 1


def count_words(total: int, increment: int) -> int:
    """Increments the total number of occurrences by the 'increment' value.

    Args:
        total: Total number of times the word has occured
        increment: Value returned by 'init_occurrences' method

    Returns:
        Number of times a word has occured
    """
    return total + increment


def switch_position(record: tuple) -> tuple:
    """Switches positions of word and count to sort by key which is count."""
    return record[1], record[0]


def main():
    text = sc.textFile('fk/datasets/book.txt')
    words = text.flatMap(normalize_words)
    word_counts = words.map(map_words).reduceByKey(count_words)
    word_counts_sorted = word_counts.map(switch_position).sortByKey()
    results = word_counts_sorted.collect()
    for result in results:
        count = str(result[0])
        word = result[1].encode('ascii', 'ignore')
        if word:
            print(f'{word}: \t\t{count}')


if __name__ == '__main__':
    main()
