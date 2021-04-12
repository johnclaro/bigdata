from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster('local').setAppName('Weather')
sc = SparkContext(conf=conf)


def parse_lines(lines: str) -> tuple:
    """Parse lines of a CSV file

    Args:
        line: Line of a CSV file

    Returns:
        A tuple containing
            0 - Station ID
            1 - Entry type
            2 - Temperature
    """
    fields = lines.split(',')
    station = fields[0]
    entry = fields[2]
    temp = float(fields[3]) * 0.1 * (9 / 5) + 32
    return (station, entry, temp)


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
    return (station, temp)


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


datasets = sc.textFile('fk/datasets/weather.csv')
lines = datasets.map(parse_lines)
min_temp = lines.filter(filter_lines)
station_temps = min_temp.map(remove_entries)
min_temps = station_temps.reduceByKey(find_lowest_temp)
results = min_temps.collect()
print('station \ttemp')
for result in results:
    station = result[0]
    temp = result[1]
    print(f'{station} \t{temp:.2f}F')
