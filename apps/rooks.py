from pyspark.sql.dataframe import DataFrame

from helpers.heatmap import get_piece, group_by_piece


def extract(df: DataFrame):
    df = get_piece(df, 'R', 'Rooks')
    df = group_by_piece(df, 'Rooks')
    return df
