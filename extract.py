import pandas as pd
pd.options.mode.chained_assignment = None

def extract_from_tsv(file_name, cols, dtypes):
    file = file_name + ".tsv.gz"
    df = pd.read_csv(file, compression='gzip', sep='\t', usecols=cols, dtype=dtypes)
    return df

def create_df(file_name, cols):
    df = pd.DataFrame()
    df = extract_from_tsv(file_name, cols)
    return df