import pandas as pd
from pandas import DataFrame

def current_holdings(c, table_name="holdings", tickers_to_drop=None) -> DataFrame:
    """
    Table with current holdings at most recent available date.
    Dynamically infers columns from the DB cursor instead of hardcoded lists.
    """
    QUERY = f"select * from {table_name} order by ref_date desc limit 1"
    cursor = c.execute(QUERY)
    cols = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=cols)

    if df.empty:
        return df

    # For backward compatibility / display names
    df = df.rename({"ref_date": "Date"}, axis=1)

    # Drop columns not intended for final table view
    for col in ["week", "day"]:
        if col in df.columns:
            df = df.drop(col, axis=1)

    if tickers_to_drop:
        for t in tickers_to_drop:
            if t in df.columns:
                df = df.drop(t, axis=1)

    return df.round(2)
