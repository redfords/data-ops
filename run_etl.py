import pandas as pd
pd.options.mode.chained_assignment = None

def extract_from_tsv(file_name, cols, dtypes):
    file = file_name + ".tsv.gz"
    df = pd.read_csv(file, compression='gzip', sep='\t', usecols=cols, dtype=dtypes)
    return df

def create_df(file_name, cols, dtypes):
    df = pd.DataFrame()
    df = extract_from_tsv(file_name, cols, dtypes)
    return df

def extract():
    movie = create_df('title.basics', [0,1,5,7,8], {'tconst': 'string', 'titleType': 'string', 'genres': 'string'})
    ratings = create_df('title.ratings', [0,1,2], {'tconst': 'string', 'averageRating': float, 'numVotes': int})
    director = create_df('title.crew', [0,1], {'tconst': 'string', 'directors': 'string'})
    writer = create_df('title.crew', [0,2], {'tconst': 'string', 'writers': 'string'})

    # keep movies only
    movie.drop(movie.loc[movie['titleType']!='movie'].index, inplace=True)
    movie.drop(['titleType'], axis=1, inplace=True)

    # keep movies released between 2015 and 2020
    years = ['2015', '2016', '2017', '2018', '2019', '2020']
    movie = movie[movie['startYear'].isin(years)]
 
    # reset df index
    movie.reset_index(drop=True, inplace=True)

    # fill in missing values
    movie['runtimeMinutes'] = pd.to_numeric(movie['runtimeMinutes'], errors='coerce')
    movie['runtimeMinutes'].fillna(int(movie['runtimeMinutes'].mean()), inplace=True)

    # add genre column
    movie.genres = movie.genres.str.split(',')
    movie = movie.explode('genres')

    # merge movie and director
    movie_director = pd.merge(movie[['tconst', 'startYear', 'genres']], director, on='tconst')
    
    movie_director.directors = movie_director.directors.str.split(',')
    movie_director = movie_director.explode('directors')

    movie_director.drop(['tconst'], axis=1, inplace=True)
    movie_director.drop_duplicates(inplace=True)

    # merge movie and writer
    movie_writer = pd.merge(movie[['tconst', 'startYear', 'genres']], writer, on='tconst')

    movie_writer.writers = movie_writer.writers.str.split(',')
    movie_writer = movie_writer.explode('writers')

    movie_writer.drop(['tconst'], axis=1, inplace=True)
    movie_writer.drop_duplicates(inplace=True)

    # merge movie and ratings
    movie_ratings = pd.merge(movie, ratings, on='tconst')

    # group and run aggregations
    g_movie_ratings = movie_ratings.groupby(['startYear', 'genres']).agg({
        'runtimeMinutes': 'mean',
        'averageRating': 'mean',
        'numVotes': 'sum'}
        ).reset_index()    

    print(g_movie_ratings.head(10))
    print(movie_director.head())
    print(movie_writer.head())

    

extract()



