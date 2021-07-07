import pandas as pd
pd.options.mode.chained_assignment = None

def extract_from_tsv(url, cols):
    df = pd.read_csv(url, compression='gzip', sep='\t', usecols=cols)
    return df

def create_df(url, cols):
    df = pd.DataFrame()
    df = extract_from_tsv(url, cols)
    return df