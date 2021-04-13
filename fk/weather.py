from pyspark import SparkConf, SparkContext


def parse_csv(row: str) -> tuple:
    """Parse lines of a CSV file

    Args:
        row: Line of a CSV file

    Returns:
        A tuple containing
            0 - Station ID
            1 - Entry type
            2 - Temperature
    """
    fields = row.split(',')
    station = fields[0]
    entry = fields[2]
    temp = float(fields[3]) * 0.1 * (9 / 5) + 32
    return station, entry, temp


def filter_lines(line: str) -> bool:
    """Filters a line by checking whether the string 'TMIN' exists.

    Args:
        line: Parsed line of a CSV file

    Returns:
        True if 'TMIN' exists otherwise False
    """
    entry = line[1]
    return 'TMIN' in entry


def remove_entries(line: str) -> tuple:
    """Removes the 'TMIN' entry fields and only return station and temp

    Args:
        line: Filtered line of a CSV file

    Returns:
        A tuple containing
            0 - Station
            1 - Temperature
    """
    station, temp = line[0], line[2]
    return station, temp


def find_lowest_temp(lowest_temp: float, next_temp: float) -> float:
    """Compare all temperature for each station then determine if the next
    temperature is lower than the current lowest temperature.

    Args:
        lowest_temp: Current lowest temperature
        next_temp: Next temperature found

    Returns:
        The lowest or minimum temperature for a given station
    """
    return min(lowest_temp, next_temp)


def main():
    conf = SparkConf().setMaster('local').setAppName('Weather')
    sc = SparkContext(conf=conf)
    data = sc.textFile('fk/datasets/weather.csv')
    weathers = data.map(parse_csv).\
        filter(filter_lines).\
        map(remove_entries).\
        reduceByKey(find_lowest_temp)
    for element in weathers.collect():
        station, temp = element
        print(f'{station} \t{temp:.2f}F')


if __name__ == '__main__':
    main()
