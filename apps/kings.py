from pyspark.sql.dataframe import DataFrame

from helpers.heatmap import get_turns, group_by_ply


def extract(df: DataFrame):
    df = get_turns(df, 'K', 'Kings')
    df = group_by_ply(df, 'Kings')
    return df
