import collections

from pyspark import SparkConf, SparkContext


def parse_data(x):
    return x.split()[2]


def main():
    conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')
    sc = SparkContext(conf=conf)

    lines = sc.textFile('fk/datasets/ratings/u.data')
    ratings = lines.map(parse_data)
    result = ratings.countByValue()

    results = collections.OrderedDict(sorted(result.items()))
    for key, value in results.items():
        print(f'{key} {value}')


if __name__ == '__main__':
    main()
