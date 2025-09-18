from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy.sql import text

from gainly.lit.util import row_selector
from gainly.portfolio import PortfolioPerformance
from gainly.yahoo import YahooFinance


conn = st.connection('transactions', type='sql', ttl=0)
with conn.session as session:
    # Initialize database:
    session.execute(text('''PRAGMA foreign_keys = ON'''))
    session.execute(text(
        '''
        CREATE TABLE IF NOT EXISTS txn
        (
            trade_date TIMESTAMP NOT NULL,
            symbol     VARCHAR   NOT NULL,
            name       VARCHAR,
            isin       VARCHAR,
            quantity   FLOAT     NOT NULL,
            price      FLOAT     NOT NULL,
            broker     VARCHAR,
            PRIMARY KEY (trade_date, symbol, broker)
        );
        '''))
    session.commit()


st.set_page_config(page_title='Cap gains tracker', page_icon=':chart_with_upwards_trend:', layout="wide")
st.markdown(r"""<style>
                .stAppDeployButton { visibility: hidden; }
                </style>
                """, unsafe_allow_html=True)

if 'hide_amounts' not in st.session_state:
    st.session_state['hide_amounts'] = False
with st.popover('Options'):
    st.checkbox('Hide amounts in graphs', key='hide_amounts')


@st.cache_data
def get_eod_prices(symbol: str, date_from: date, date_to: date) -> pd.DataFrame:
    return YahooFinance().get_oed_prices(symbol, date_from, date_to)

txns = conn.query('''SELECT * FROM TXN ORDER BY trade_date ASC''', ttl=0)
txns['trade_date'] = pd.to_datetime(txns['trade_date'])
txns = row_selector(txns, st_cols=st.columns(2), key='txns', df_cols=['symbol', 'broker'])(txns)

metric = st.container()
chart = st.container()
pos = st.container()
trades = st.container()

portfolio = PortfolioPerformance(txns, YahooFinance())
valuations = portfolio.daily_valuations()
positions = portfolio.positions().reset_index()
pl = positions['pl'].sum()


with metric:
    st.metric('Portfolio value',
              f"€{positions['value'].sum():,.2f}",
              f"{pl / positions['invested'].sum() * 100:.2f}% | €{pl:,.2f}",
              border=True, width='content')

with pos, st.expander('Positions'):
    st.dataframe(positions[['symbol', 'position', 'value', 'pl']],
                 use_container_width=False, hide_index=True,
                 column_config={
                     'position': st.column_config.NumberColumn(format='localized'),
                     'value': st.column_config.NumberColumn(format='euro'),
                     'pl': st.column_config.NumberColumn(format='euro'),
                 })

with chart:
    df = (valuations[['date', 'invested', 'pl']]
          .rename(columns={'pl': 'returns'})
          .groupby('date')
          .sum()
          .reset_index()
          .melt(id_vars=['date'], var_name='type', value_name='value'))

    fig = px.area(df, x="date", y="value", color="type", line_group='type')
    for dt in valuations[valuations['price'] > 0]['date'].unique():
        fig.add_vline(x=dt, line_dash="dot", line_color="black")
    fig.update_layout(
        xaxis=dict(showgrid=True, tickformat="%d %b %Y", tickangle=45, title=''),
        yaxis=dict(title='€', showticklabels=not st.session_state['hide_amounts']))
    st.plotly_chart(fig, use_container_width=True)

with trades, st.expander('Trade history'):
    st.dataframe(txns, use_container_width=True)
