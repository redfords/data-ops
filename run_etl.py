import pandas as pd
pd.options.mode.chained_assignment = None
import extract as e
import transform as t

def run_etl():
    
    # Create df from datasets
    movie = e.create_df('movie')
    director = e.create_df('director')
    writer = e.create_df('writer')
    ratings = e.create_df('ratings')
    name = e.create_df('name')

    # Filter by format
    movie = movie[movie['titleType'] == 'movie']
    movie.drop(['titleType'], axis=1, inplace=True)

    # Filter by release date
    years = ['2015', '2016', '2017', '2018', '2019', '2020']
    movie = movie[movie['startYear'].isin(years)]
    movie.reset_index(drop=True, inplace=True)

    # Fix missing values
    movie['runtimeMinutes'] = pd.to_numeric(
        movie['runtimeMinutes'],
        errors='coerce')
    movie.dropna(subset = ['runtimeMinutes'], inplace=True)

    movie['genres'] = movie['genres'].str.lower()
    movie['genres'] = movie['genres'].str.replace(
        r'\\n','other', regex=True)

    # Split column into rows
    movie = t.list_to_row(movie, 'genres')
    
    # Merge crew and movie
    movie_director = t.merge(movie, director)
    movie_writer = t.merge(movie, writer)

    # Fix missing values
    movie_director['directors'] = movie_director['directors'].str.lower()
    movie_director = movie_director[
        ~movie_director['directors'].str.contains(r'\\n')]

    movie_writer['writers'] = movie_writer['writers'].str.lower()
    movie_writer = movie_writer[
        ~movie_writer['writers'].str.contains(r'\\n')]

    # Split director and writer into rows
    movie_director = t.list_to_row(movie_director, 'directors')
    movie_writer = t.list_to_row(movie_writer, 'writers')

    # Get top directors by year and genre
    top_director = t.group_by_count(movie_director, 'tconst')
    
    # Merge director and name
    columns = {'nconst': 'directors', 'primaryName': 'name'}
    name = t.rename_cols(name, columns)

    top_director = pd.merge(
        name[['directors','name']],
        top_director[['directors','startYear','genres']],
        on='directors')

    top_director.drop(['directors'], axis=1, inplace=True)
    
    top_director = t.group_by_join(top_director)

    # Group by year and genre
    movie_director = t.group_by(movie_director, 'directors')
    movie_writer = t.group_by(movie_writer, 'writers')

    # Group and run aggregations
    movie_ratings = pd.merge(
        movie, ratings,
        on='tconst')

    movie_ratings = t.group_by_sum(movie_ratings)

    movie_ratings = t.merge_left(movie_ratings, movie_director)
    movie_ratings = t.merge_left(movie_ratings, movie_writer)
    movie_ratings = t.merge_left(movie_ratings, top_director)

    # Rename columns
    columns = {'directors':'numDirectors', 'writers':'numWriters'}
    movie_ratings = t.rename_cols(movie_ratings, columns)
    
    # Fix data type
    columns = ['runtimeMinutes', 'averageRating']
    movie_ratings[columns] = movie_ratings[columns].round(2)
    movie_ratings['startYear'] = pd.to_numeric(
        movie_ratings['startYear'], errors='coerce')

    # Load into .csv
    movie_ratings.to_csv('/home/joana/airflow/dags/resultados.csv', index=False)

run_etl()