import pandas as pd
pd.options.mode.chained_assignment = None

def create_df(file_name):
    source_url = 'https://datasets.imdbws.com/'

    files = {
        'movie': (
            'title.basics',
            [0,1,5,7,8],
            {
                'tconst': 'object',
                'titleType': 'object',
                'startYear': 'float64',
                'runtimeMinutes':'float64',
                'genres': 'object'
            }
        ),
        'crew': (
            'title.crew',
            [0,1,2],
            {
                'tconst': 'object',
                'directors': 'object',
                'writers': 'object'
            }
        ),
        'ratings': (
            'title.ratings',
            [0,1,2],
            {
                'tconst': 'object',
                'averageRating': 'float64',
                'numVotes': 'int64'
            }
        ),
        'name': (
            'name.basics',
            [0,1],
            {
                'nconst': 'object',
                'primaryName': 'object'
            }
        )
    }

    url = source_url + files[file_name][0]  + '.tsv.gz'
    cols = files[file_name][1]

    df = pd.read_csv(
        url,
        compression='gzip',
        sep='\t',
        usecols=cols,
        na_values=r"\N",
        engine='c'
    )

    return df