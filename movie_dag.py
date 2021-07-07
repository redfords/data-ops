from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
pd.options.mode.chained_assignment = None
import extract
import transform

def run_etl():
    movie = extract.create_df('https://datasets.imdbws.com/title.basics.tsv.gz', [0,1,5,7,8])

    # filter by format
    movie = movie[movie['titleType'] == 'movie']
    movie.drop(['titleType'], axis=1, inplace=True)

    # filter by release date
    years = ['2015', '2016', '2017', '2018', '2019', '2020']
    movie = movie[movie['startYear'].isin(years)]
    movie.reset_index(drop=True, inplace=True)

    # fill in missing values
    movie['runtimeMinutes'] = pd.to_numeric(movie['runtimeMinutes'], errors='coerce')
    runtime_mean = movie['runtimeMinutes'].mean()
    movie['runtimeMinutes'].fillna(int(runtime_mean), inplace=True)

    # add genre column
    movie['genres'] = movie['genres'].str.lower()
    movie['genres'] = movie['genres'].str.replace(r'\\n', 'other', regex=True)
    movie = transform.list_to_row(movie, 'genres')

    director = extract.create_df('https://datasets.imdbws.com/title.crew.tsv.gz', [0,1])
    writer = extract.create_df('https://datasets.imdbws.com/title.crew.tsv.gz', [0,2])

    # merge crew and movie
    movie_director = pd.merge(movie[['tconst', 'startYear', 'genres']], director, on='tconst')
    movie_writer = pd.merge(movie[['tconst', 'startYear', 'genres']], writer, on='tconst')

    # add director and writer column
    movie_director = transform.list_to_row(movie_director, 'directors')
    movie_writer = transform.list_to_row(movie_writer, 'writers')

    # group by year and genre
    movie_director = transform.group_by(movie_director, 'directors')
    movie_writer = transform.group_by(movie_writer, 'writers')

    ratings = extract.create_df('https://datasets.imdbws.com/title.ratings.tsv.gz', [0,1,2])

    # merge movie and ratings
    movie_ratings = pd.merge(movie, ratings, on='tconst')

    # group and run aggregations
    movie_ratings = movie_ratings.groupby(['startYear', 'genres']).agg({
        'runtimeMinutes': 'mean',
        'averageRating': 'mean',
        'numVotes': 'sum'}
        ).reset_index()

    # add director and writer
    movie_ratings = pd.merge(movie_ratings, movie_director, on=['startYear', 'genres'], how='left').fillna(0)
    movie_ratings = pd.merge(movie_ratings, movie_writer, on=['startYear', 'genres'], how='left').fillna(0)
    
    # round and change data type
    columns = ['runtimeMinutes', 'averageRating']
    movie_ratings[columns] = movie_ratings[columns].round(2)
    movie_ratings['startYear'] = pd.to_numeric(movie_ratings['startYear'], errors='coerce')

    print(movie_ratings.head(100))

    movie_ratings.to_csv('/home/joana/airflow/dags/resultados.csv', index=False)


# define the default dag arguments
default_args = {
	'owner': 'joana',
	'depends_on_past': False,
	'email': ['joanapiovaroli@gmail.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 5,
	'retry_delay': timedelta(minutes = 1)
	}

# define the dag, start date and frequency
dag = DAG(
	dag_id = 'movie_dag',
	default_args = default_args,
	start_date = datetime(2021,7,7),
	schedule_interval = timedelta(minutes = 1440)
	)

# run etl process
task1 =  PythonOperator(
	task_id = 'run_etl',
	provide_context = True,
	python_callable = run_etl,
	dag = dag
	)

