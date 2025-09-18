from functools import reduce, partial
from typing import Callable, Literal

import pandas as pd
import streamlit as st


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


def align(content: str, direction: Literal['right', 'center'], nowrap=False, unsafe_allow_html=False):
    st.markdown(f'<div style="text-align: {direction}; width: 100%; {"white-space: nowrap;" if nowrap else ""}">'
                f'{content if unsafe_allow_html else html.escape(content)}</div>',
                unsafe_allow_html=True)
