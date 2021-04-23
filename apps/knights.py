from pyspark.sql.dataframe import DataFrame

from helpers.pieces import get_piece, group_by_piece


def extract(df: DataFrame):
    df = get_piece(df, 'N')
    df = group_by_piece(df)
    return df
