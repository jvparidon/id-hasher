import os
import pandas as pd
import hashlib as hl


def create_salt():
    return os.urandom(hl.blake2b.SALT_SIZE)

def hash_id(id_string, salt, hash_size=8):
    return hl.blake2b(str(id_string).encode('utf8'), digest_size=hash_size, salt=salt).hexdigest()

def hash_series(series, salt, hash_size=8):
    return series.apply(lambda x: hash_id(x, salt, hash_size))

def hash_df(df, colname, salt, hash_size=8, drop=False):
    df = df.copy(deep=True)
    if drop:
        df[colname] = hash_series(df[colname], salt, hash_size)
    else:
        newcol = colname + '_hashed'
        df[newcol] = hash_series(df[colname], salt, hash_size)
    return df

def hash_file(filename, colname, salt, hash_size=8, drop=False, sep='\t'):
    df = pd.read_csv(filename, sep=sep)
    df = hash_df(df, colname, salt, hash_size, drop)
    df.to_csv(filename, sep=sep, index=False)
    return df