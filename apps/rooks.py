from pyspark.sql.dataframe import DataFrame

from helpers.pieces import find_pieces, group_by_location


def extract(df: DataFrame):
    df = find_pieces(df, 'R')
    df = group_by_location(df)
    return df
