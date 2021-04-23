from pyspark.sql.dataframe import DataFrame

from helpers.timer import timer


def is_ply(ply):
    return (
        '.' not in ply and
        '{' not in ply and
        '}' not in ply and
        '%' not in ply
    )


def reformat(partition, schema: dict):
    for row in partition:
        value = row.value
        if '"' in value:
            text = value.split('"')
            column = text[0][1:].replace(' ', '')
            if column in schema.keys():
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
                schema[column] = record
        if '1. ' in value:
            notations = [
                ply
                for ply in value.split(' ')
                if is_ply(ply)
            ]
            schema['Notations'] = notations
            yield list(schema.values())


@timer
def transform(data: DataFrame):
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
        'Notations': [],
    }

    df = data.\
        rdd.\
        mapPartitions(lambda partition: reformat(partition, schema)).\
        toDF(list(schema.keys()))

    return df

