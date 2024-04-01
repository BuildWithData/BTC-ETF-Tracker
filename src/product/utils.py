from pandas import DataFrame
import pandas as pd
from typing import Union


pd.options.mode.copy_on_write = True


def get_new_rows(df1, df2, keys) -> Union[DataFrame, None]:
    """
    join left anti + drop duplicates to keep records with distinct values only

    drop duplicates -> if 2 consecutive runs have the same exact data, the only difference
                       is the column file_name/timestamp -> keep just one
    """

    if isinstance(keys, str):
        keys = [keys]
    df2 = df2[keys]
    df2["flag"] = 1
    tmp = df1.merge(df2, on=keys, how="left")
    new = tmp[pd.isna(tmp["flag"])]
    new = new.drop_duplicates([c for c in new.columns if c != "file_name"])
    new = new.drop("flag", axis=1).reset_index(drop=True)
    if new.shape[0] == 0:
        new = None

    return new


def get_rows_to_update(new, old, keys) -> Union[DataFrame, None]:

    if isinstance(keys, str):
        keys = [keys]

    same_keys = new.merge(old[keys], on=keys)
    same_keys = [same_keys[same_keys.index == i].reset_index(drop=True) for i in same_keys.index]

    cols = [c for c in old.columns if c != 'file_name']
    out = [df for df in same_keys if df[cols].equals(old[cols]) is False]

    if len(out) > 0:
        out = (
            pd.concat(out)
            .sort_values("file_name", ascending=False)
            .drop_duplicates(keys)
            .reset_index(drop=True)
        )
    else:
        out = None

    return out


def get_insert_query(df, table_name) -> str:

    row = df.sort_values("file_name", ascending=False).reset_index(drop=True).loc[0].values

    QUERY = f"INSERT INTO {table_name} VALUES ("

    for v in row:
        if isinstance(v, int) or isinstance(v, float):
            QUERY += f"{v},"
        else:
            QUERY += f"'{v}',"

    # drop last comma and add )
    QUERY = QUERY[:-1] + ")"

    return QUERY


def get_update_query(df, table_name, keys: Union[str, list[str]]) -> str:

    row = df.loc[0]

    QUERY = f"UPDATE {table_name} SET "

    for v, c in zip(row, row.index):
        if isinstance(v, int) or isinstance(v, float):
            QUERY += f"{c}={v},"
        else:
            QUERY += f"{c}='{v}',"

    QUERY = QUERY[:-1] + " WHERE"

    for v, c in zip(row, row.index):
        if c in keys:
            QUERY += f" {c}='{v}',"

    QUERY = QUERY[:-1] # strip last comma

    return QUERY


def get_last_row_from_table(table_name, keys: Union[str, list[str]], columns, con) -> DataFrame:

    QUERY = f"select * from {table_name} order by"

    if isinstance(keys, str):
        QUERY += f" {keys} desc limit 1"

    else:
        QUERY += " {} limit 1".format(" desc, ".join(keys))

    db = list(con.execute(QUERY))
    return DataFrame(db, columns=columns)
