from collections import deque


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
    new_df = deque()
    columns = schema.copy()
    for row in df:
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
