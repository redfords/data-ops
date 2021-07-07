import pandas as pd
pd.options.mode.chained_assignment = None

def list_to_row(data, column):
    data[column] = data[column].str.split(',')
    data = data.explode(column)
    return data

def group_by(data, column):
    data.drop(['tconst'], axis=1, inplace=True)
    data.drop_duplicates(inplace=True)
    data = data.groupby(['startYear', 'genres']).agg({column: 'count'}).reset_index()
    return data

def rename_cols(data, cols):
    data.rename(columns = cols, inplace = True)
    return data