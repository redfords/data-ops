import pandas as pd
pd.options.mode.chained_assignment = None

def list_to_row(data, column):
    data[column] = data[column].str.split(',')
    data = data.explode(column)
    return data

def group_by_count(data, column):
    data = data.groupby(
        ['startYear', 'genres', 'directors'])[column].count().reset_index(
            name="count")

    index = data.groupby(
        ['startYear', 'genres'])['count'].transform(max) == data['count']

    data = data[index]
    return data

def group_by_join(data):
    data = data.groupby(
        ['startYear','genres'])['primaryName'].apply(
            '; '.join).reset_index()
    return data

def rename_cols(data, cols):
    data.rename(columns = cols, inplace = True)
    return data