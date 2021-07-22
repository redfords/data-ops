from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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
    joined_data = movie.join(
        ratings.set_index('tconst'), on='tconst', how='inner'
        ).join(
        director.set_index('tconst'), on='tconst', how='left'
        ).join(
        writer.set_index('tconst'), on='tconst', how='left'
    )

    # Create name df
    name = e.create_df('name')

    # Get top directors by year and genre
    top_director = joined_data[['tconst', 'startYear', 'genres', 'directors']]
    top_director.drop_duplicates(inplace=True)
    top_director = t.group_by_count(top_director, 'tconst')
    top_director = top_director.join(
        name.set_index('nconst'), on='directors', how='inner'
    )
    top_director.drop(['directors'], axis=1, inplace=True)
    top_director = t.group_by_join(top_director)

    # Run aggregations count distinct
    aggregate = {
        'directors': 'nunique',
        'writers': 'nunique'
    }

    result = joined_data.groupby(
        ['startYear', 'genres']).agg(aggregate).reset_index()

    # Run aggregations sum and mean
    joined_data.drop(['directors', 'writers'], axis=1, inplace=True)
    joined_data.drop_duplicates(inplace=True)

    aggregate = {
        'runtimeMinutes': 'mean',
        'averageRating': 'mean',
        'numVotes': 'sum',
    }

    result_ratings = joined_data.groupby(
        ['startYear', 'genres']).agg(aggregate).reset_index()

    # Add ratings data
    result = pd.merge(
        result, result_ratings, on=['startYear', 'genres'], how='left'
    )
  
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

    # Reorder columns
    columns = [
        'startYear',
        'genres',
        'runtimeMinutes',
        'averageRating',
        'numVotes',
        'numDirectors',
        'numWriters',
        'topDirectors'
    ]
    result = result[columns]

    result.to_csv('/home/airflow/dags/resultados.csv', index=False)

default_args = {
	'owner': 'joana',
	'depends_on_past': False,
	'email': ['joanapiovaroli@gmail.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 5,
	'retry_delay': timedelta(minutes = 1)
}

dag = DAG(
	dag_id = 'movie_dag',
	default_args = default_args,
	start_date = datetime(2021,7,7),
	schedule_interval = timedelta(minutes = 1440)
)

task1 =  PythonOperator(
	task_id = 'run_etl',
	provide_context = True,
	python_callable = run_etl,
	dag = dag
)