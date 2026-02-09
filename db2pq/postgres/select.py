import ibis.selectors as s

def apply_keep_drop(df, *, keep=None, drop=None):
    if drop:
        df = df.drop(s.matches(drop))
    if keep:
        df = df.select(s.matches(keep))
    return df