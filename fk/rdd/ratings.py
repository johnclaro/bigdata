import collections

from pyspark import SparkConf, SparkContext


def parse_data(x):
    return x.split()[2]


def main():
    conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')
    sc = SparkContext(conf=conf)
    data = sc.textFile('fk/datasets/ratings/u.data')
    rdd = data.map(parse_data).countByValue()
    rdd = collections.OrderedDict(sorted(rdd.items()))
    for key, value in rdd.items():
        print(f'{key} {value}')


if __name__ == '__main__':
    main()
