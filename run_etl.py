import pandas as pd
pd.options.mode.chained_assignment = None

def extract_from_tsv(file_name, cols):
    file = file_name + ".tsv.gz"
    df = pd.read_csv(file, compression='gzip', sep='\t', usecols=cols)
    return df

def create_df(file_name, cols):
    df = pd.DataFrame()
    df = extract_from_tsv(file_name, cols)
    return df

def extract():
    movie = create_df('title.basics', [0,1,5,7,8])
    print(movie.head())

    ratings = create_df('title.ratings', [0,1,2])
    print(ratings.head())

extract()
