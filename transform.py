import pandas as pd
pd.options.mode.chained_assignment = None

def list_to_row(data, column):
    data[column] = data[column].str.split(',')
    data = data.explode(column)
    return data

def group_by(data, column):
    data.drop(['tconst'], axis=1, inplace=True)
    data.drop_duplicates(inplace=True)
    data = data.groupby(
        ['startYear', 'genres']).agg(
            {column: 'count'}).reset_index()
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
        ['startYear','genres'])['name'].apply(
            ', '.join).reset_index()
    return data

def group_by_sum(data):
    data = data.groupby(
        ['startYear', 'genres']).agg({
        'runtimeMinutes': 'mean',
        'averageRating': 'mean',
        'numVotes': 'sum'}).reset_index()
    return data

def merge(left, right):
    data = pd.merge(
        left[['tconst', 'startYear', 'genres']], right,
        on='tconst')
    return data

def merge_left(left, right):
    data = pd.merge(
        left, right,
        on=['startYear', 'genres'],
        how='left'
        ).fillna(0)
    return data

def rename_cols(data, cols):
    data.rename(columns = cols, inplace = True)
    return data