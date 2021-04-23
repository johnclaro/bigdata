from pyspark.sql.dataframe import DataFrame

from helpers.heatmap import get_piece, group_by_piece


def extract(df: DataFrame):
    df = get_piece(df, 'Q', 'Queens')
    df = group_by_piece(df, 'Queens')
    return df
