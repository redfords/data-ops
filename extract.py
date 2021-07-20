import pandas as pd
pd.options.mode.chained_assignment = None

def extract_from_tsv(url, cols):
    df = pd.read_csv(url, compression='gzip', sep='\t', usecols=cols)
    return df

def create_df(file_name):
    source_url = 'https://datasets.imdbws.com/'

    files = {
        'movie': ('title.basics', [0,1,5,7,8]),
        'director': ('title.crew', [0,1]),
        'writer': ('title.crew', [0,2]),
        'ratings': ('title.ratings', [0,1,2]),
        'name': ('name.basics', [0,1])
    }

    url = source_url + files[file_name][0]  + '.tsv.gz'
    cols = files[file_name][1]

    df = pd.DataFrame()
    df = extract_from_tsv(url, cols)
    return df