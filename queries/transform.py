from timeit import default_timer as timer
from datetime import timedelta

from pyspark.sql import SparkSession


default_columns = {
    'Event': '',
    'Site': '',
    'White': '',
    'Black': '',
    'Result': '',
    'UTCDate': '',
    'UTCTime': '',
    'WhiteElo': '',
    'BlackElo': '',
    'WhiteRatingDiff': '',
    'BlackRatingDiff': '',
    'WhiteTitle': '',
    'BlackTitle': '',
    'ECO': '',
    'Opening': '',
    'TimeControl': '',
    'Termination': '',
    'Notations': '',
}


def reformat(df):
    new_df = []
    columns = default_columns.copy()
    for row in df:
        value = row.value
        if '"' in value:
            text = value.split('"')
            key = text[0][1:].replace(' ', '')
            columns[key] = text[1]
        if '1. ' in value:
            columns['Notations'] = value
            dvalues = list(columns.values())
            if len(dvalues) != 18:
                import json
                print(json.dumps(columns, indent=4))
            new_df.append(dvalues)
            columns = default_columns.copy()
    return iter(new_df)


def transform(data):
    df = data.\
        rdd.\
        mapPartitions(reformat).\
        toDF(list(default_columns.keys()))

    return df


def main():
    filename = '93mb.pgn'
    start = timer()
    spark = SparkSession.builder.appName('openings').getOrCreate()
    data = spark.read.text(f'datasets/{filename}')
    df = transform(data)

    extract_timer = timer()
    print(f'Extracting {filename}: {timedelta(seconds=timer() - start)}')
    # df.show(20, truncate=False)
    df.coalesce(4).write.mode('overwrite').csv('files/transform')
    print(f'End {filename}: {timedelta(seconds=timer() - extract_timer)}')

    spark.stop()


if __name__ == '__main__':
    main()
