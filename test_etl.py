import pandas as pd
pd.options.mode.chained_assignment = None
import extract as e
import transform as t

def run_etl():
    
    # Create movie df
    movie = e.create_df('movie')

    # Filter by format
    movie = movie[movie['titleType'] == 'movie']
    movie.drop(['titleType'], axis=1, inplace=True)

    # Fix missing data
    movie.dropna(subset=['startYear', 'runtimeMinutes'], inplace=True)
    movie['genres'].fillna('Other', inplace=True)

    # Change dtype
    movie['startYear'] = movie['startYear'].astype('int')
    movie['runtimeMinutes'] = movie['runtimeMinutes'].astype(float)

    # Filter by release date
    condition = (movie['startYear'] >= 2015) & (movie['startYear'] <= 2020)
    movie = movie[condition]

    # Split column into rows
    movie = t.list_to_row(movie, 'genres')

    # Create crew df
    crew = e.create_df('crew')

    director = crew[['tconst', 'directors']]
    director.dropna(subset=['directors'], inplace=True)

    writer = crew[['tconst', 'writers']]
    writer.dropna(subset=['writers'], inplace=True)

    # Split column into rows
    director = t.list_to_row(director, 'directors')
    writer = t.list_to_row(writer, 'writers')

    # Create ratings df
    ratings = e.create_df('ratings')

    # Join into single df
    result = movie.join(
        ratings.set_index('tconst'), on='tconst', how='inner'
        ).join(
        director.set_index('tconst'), on='tconst', how='left'
        ).join(
        writer.set_index('tconst'), on='tconst', how='left'
    )

    result.to_csv('/home/joana/airflow/dags/join.csv', index=False)    

    # Create name df
    name = e.create_df('name')

    # Get top directors by year and genre
    top_director = result[['tconst', 'startYear', 'genres', 'directors']]
    top_director.drop_duplicates(inplace=True)
    top_director = t.group_by_count(top_director, 'tconst')
    top_director = top_director.join(
        name.set_index('nconst'), on='directors', how='inner'
    )
    top_director.drop(['directors'], axis=1, inplace=True)
    top_director = t.group_by_join(top_director)

    # Run aggregations
    aggregate = {
        'runtimeMinutes': 'mean',
        'averageRating': 'mean',
        'numVotes': 'sum',
        'directors': 'nunique',
        'writers': 'nunique'
    }

    result = result.groupby(
        ['startYear', 'genres']).agg(aggregate).reset_index()
  
    # Fix data type
    columns = ['runtimeMinutes', 'averageRating']
    result[columns] = result[columns].round(2)

    # Add top directors
    result = pd.merge(
        result, top_director, on=['startYear', 'genres'], how='left'
    )

    # Rename columns
    columns = {
        'directors':'numDirectors',
        'writers':'numWriters',
        'primaryName': 'topDirectors'
    }
    result = t.rename_cols(result, columns)

    result.to_csv('/home/joana/airflow/dags/resultados6.csv', index=False)

run_etl()