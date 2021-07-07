import pandas as pd
pd.options.mode.chained_assignment = None
import wget

def extract_from_tsv(file_name, cols):
    file = file_name + ".tsv.gz"
    df = pd.read_csv(file, compression='gzip', sep='\t', usecols=cols)
    return df

def create_df(file_name, cols):
    df = pd.DataFrame()
    df = extract_from_tsv(file_name, cols)
    return df

def list_to_row(data, column):
    data[column] = data[column].str.split(',')
    data = data.explode(column)
    return data

def group_by(data, column):
    data.drop(['tconst'], axis=1, inplace=True)
    data.drop_duplicates(inplace=True)
    data = data.groupby(['startYear', 'genres']).agg({column: 'count'}).reset_index()
    return data  

def download_files():
    url = {
        'https://datasets.imdbws.com/title.basics.tsv.gz': 'title.basics.tsv.gz',
        'https://datasets.imdbws.com/title.crew.tsv.gz': 'title.crew.tsv.gz',
        'https://datasets.imdbws.com/title.ratings.tsv.gz': 'title.ratings.tsv.gz'
    }

    for key, value in url.items():
        wget.download(key, value)

def extract():
    movie = create_df('title.basics', [0,1,5,7,8])

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
    movie = list_to_row(movie, 'genres')

    director = create_df('title.crew', [0,1])
    writer = create_df('title.crew', [0,2])

    # merge crew and movie
    movie_director = pd.merge(movie[['tconst', 'startYear', 'genres']], director, on='tconst')
    movie_writer = pd.merge(movie[['tconst', 'startYear', 'genres']], writer, on='tconst')

    # add director and writer column
    movie_director = list_to_row(movie_director, 'directors')
    movie_writer = list_to_row(movie_writer, 'writers')

    # group by year and genre
    movie_director = group_by(movie_director, 'directors')
    movie_writer = group_by(movie_writer, 'writers')

    ratings = create_df('title.ratings', [0,1,2])

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

    movie_ratings.to_csv('resultados.csv', index=False)


download_files()
extract()