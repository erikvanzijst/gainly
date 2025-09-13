from functools import reduce, partial
from typing import Callable

import pandas as pd


def row_selector(reps: pd.DataFrame, st_cols: list, df_cols: list[str], allow_all: bool = True, key: str = 'selector') \
        -> Callable[[pd.DataFrame], pd.DataFrame]:
    def selector(colname: str, colval: str, q, df: pd.DataFrame) -> Callable[[pd.DataFrame], pd.DataFrame]:
        return q(df if colval == 'All' else df[df[colname] == colval])

    base = ['All'] if allow_all else []
    return reduce(
        lambda q, col:
            partial(selector, col[1],
                    col[0].selectbox(col[1], key=f'{key} {col[1]}', options=base + list(q(reps)[col[1]].sort_values().unique())), q),
        zip(st_cols, df_cols),
        lambda df: df)
