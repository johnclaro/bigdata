from pyspark.sql.dataframe import DataFrame

from helpers.heatmap import get_turns, group_by_ply


def extract(df: DataFrame):
    df = get_turns(df, 'Q', 'Queens')
    df = group_by_ply(df, 'Queens')
    return df
