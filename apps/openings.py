from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import Bucketizer

from helpers.timer import timer


@timer
def filter_elos(df):
    """Filters out null WhiteElo and BlackElo values.

    Args:
        df: | WhiteElo | BlackElo |
            |----------|----------|
            | null     | 24       |
            | 32       | 23       |

    Returns:
            | WhiteElo | BlackElo | Elo  |
            |----------|----------|------|
            | 32       | 23       | 27.5 |
    """
    df = df.\
        filter(
            (f.col('BlackElo').isNotNull()) &
            (f.col('WhiteElo').isNotNull())
        )
    return df


@timer
def calculate_avg_elo(df):
    """Calculates average of `WhiteElo` and `BlackElo`.

    Args:
        df: | WhiteElo | BlackElo |
            |----------|----------|
            | 1200     | 1600     |
            | 2000     | 2600     |

    Returns:
            | WhiteElo | BlackElo | Elo  |
            |----------|----------|------|
            | 1200     | 1600     | 1400 |
            | 2000     | 2600     | 2300 |
    """
    columns = (
        f.col(name) for name in ('WhiteElo', 'BlackElo')
    )
    average = sum(column for column in columns) / 2
    df = df.\
        withColumn(
            'Elo',
            average,
        )
    return df


@timer
def setup_elo_range_ids(df):
    """Setups `EloRangeId` by determining which split the elo is in.

    Args:
        df: | WhiteElo | BlackElo | Elo  |
            |----------|----------|------|
            | 1200     | 1600     | 1400 |
            | 2000     | 2600     | 2300 |

    Returns:
            | Elo  | EloRangeId |
            |------|------------|
            | 1400 | 0.0        |
            | 2300 | 2.0        |
    """
    splits = [
        0,
        1500,
        2000,
        2500,
        9999,
    ]
    buckets = Bucketizer(
        splits=splits,
        inputCol='Elo',
        outputCol='EloRangeID',
    )
    df = buckets.transform(df)
    return df


@timer
def assign_elo_range_labels(df):
    """Assigns the label names of `EloRangeId`.

    Args:
        df: | Elo  | EloRangeId |
            |------|------------|
            | 1400 | 0.0        |
            | 2300 | 2.0        |

    Returns:
            | Elo  | EloRangeId | EloRange  |
            |------|------------|-----------|
            | 1400 | 0.0        | Below1500 |
            | 2300 | 2.0        | 2000-2500 |
    """
    default_labels = (
        'Below1500',
        '1500-2000',
        '2000-2500',
        'Above2500',
    )
    labels = f.array(
        *(f.lit(label) for label in default_labels)
    )
    df = df.\
        withColumn(
            'EloRange',
            labels.getItem(
                f.col('EloRangeID').cast('integer')
            )
        )

    return df


@timer
def group_openings(df):
    """Groups the `Openings` then pivots the values of `EloRange` by count.

    Args:
        df: | Opening        | EloRange  |
            |----------------|-----------|
            | Ruy Lopez      | Below1500 |
            | Italian's Game | 2000-2500 |
            | Italian's Game | Below1500 |

    Returns:
            | Opening        | Below1500 | 2000-2500 |
            |----------------|-----------|-----------|
            | Ruy Lopez      | 1         | 0         |
            | Italian's Game | 1         | 1         |
    """
    df.select('Opening', 'EloRange').show()
    df = df. \
        groupBy('Opening'). \
        pivot('EloRange'). \
        count(). \
        na. \
        fill(0)

    return df


@timer
def calculate_total(df):
    """Calculates `Total` value of the frequency of `Opening` by `EloRange`.

    Args:
        df: | Opening        | Below1500 | 2000-2500 |
            |----------------|-----------|-----------|
            | Ruy Lopez      | 1         | 0         |
            | Italian's Game | 1         | 1         |

    Returns:
            | Opening        | Below1500 | 2000-2500 | Total |
            |----------------|-----------|-----------|-------|
            | Ruy Lopez      | 1         | 0         | 1     |
            | Italian's Game | 1         | 1         | 2     |
    """
    columns = (
        f.col(name) for name in df.columns[1:]
    )
    df = df.\
        withColumn(
            'Total',
            sum(column for column in columns)
        ). \
        orderBy(
            f.desc(
                f.col('Total')
            )
        ). \
        limit(10)

    return df


def extract(df: DataFrame):
    df = filter_elos(df)
    df = calculate_avg_elo(df)
    df = setup_elo_range_ids(df)
    df = assign_elo_range_labels(df)
    df = group_openings(df)
    df = calculate_total(df)
    return df
