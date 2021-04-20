from timeit import default_timer as timer
from datetime import timedelta

from pyspark.sql import SparkSession


schema = {
    'Event': '',
    'Site': '',
    'White': '',
    'Black': '',
    'Result': '',
    'UTCDate': '',
    'UTCTime': '',
    'WhiteElo': 0,
    'BlackElo': 0,
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
    columns = schema.copy()
    for row in df:
        value = row.value
        if '"' in value:
            text = value.split('"')
            column = text[0][1:].replace(' ', '')
            record = text[1]
            if column in ('WhiteElo', 'BlackElo'):
                try:
                    record = int(record)
                except ValueError:
                    pass
            elif column == 'Event':
                record = record.split(' ')[1]
            elif column == 'Site':
                record = record.split('/')[-1]
            columns[column] = record
        if '1. ' in value:
            columns['Notations'] = value
            new_df.append(list(columns.values()))
            columns = schema.copy()
    return iter(new_df)


def transform(data):
    df = data.\
        rdd.\
        mapPartitions(reformat).\
        toDF(list(schema.keys()))

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
    df.coalesce(4).write.mode('overwrite').parquet('files/transform')
    print(f'End {filename}: {timedelta(seconds=timer() - extract_timer)}')

    spark.stop()


if __name__ == '__main__':
    main()
