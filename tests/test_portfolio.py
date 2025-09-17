from datetime import datetime, date
from io import StringIO
from textwrap import dedent

import pandas as pd
import pytest

from gainly.eod import QuoteFetcher, NullQuoteFetcher
from gainly.portfolio import PortfolioPerformance


class MockQuoteFetcher(QuoteFetcher):
    prices = pd.DataFrame({
            'date': [date(2024, 12, 31),
                     date(2025, 1, 1),
                     date(2025, 1, 15),
                     date(2025, 3, 15),
                     date(2025, 3, 15)],
            'symbol': ['IWDA', 'IWDA', 'IWDA', 'IWDA', 'EUNA.DE'],
            'close': [0.9, 1.1, 2, 4, 2]
        })
    def get_oed_prices(self, symbol: str, date_from: date, date_to: date) -> pd.DataFrame:
        return self.prices[self.prices['symbol'] == symbol]


daily_positions_scenarios = [
    # The first scenario tests the construction of the running positions without additional EOD prices
    (
        # no eod prices:
        NullQuoteFetcher(),
        # expected daily positions:
        (pd.read_csv(StringIO(dedent("""\
                    date,symbol,price,position,invested,close
                    2025-01-01,EUNA.DE,,,,
                    2025-01-01,IWDA,1.0,1.0,1.0,
                    2025-02-01,EUNA.DE,2.0,2.0,4.0,
                    2025-02-01,IWDA,,1.0,1.0,
                    2025-03-01,EUNA.DE,1.0,1.0,3.0,
                    2025-03-01,IWDA,3.0,5.0,11.0,
                    """)),
                     parse_dates=['date'], date_format='%Y-%m-%d')
         .pipe(lambda df: df.assign(date=df['date'].dt.date))),
    ),
    # The second scenario tests the construction of the running positions with EOD prices for additional resolution
    (
        # EOD prices:
        MockQuoteFetcher(),
        # expected daily positions with additional EOD prices:
        (pd.read_csv(StringIO(dedent("""\
                    date,symbol,price,position,invested,close
                    2024-12-31,EUNA.DE,,,,
                    2024-12-31,IWDA,,,,0.9
                    2025-01-01,EUNA.DE,,,,
                    2025-01-01,IWDA,1.0,1.0,1.0,1.1
                    2025-01-15,EUNA.DE,,,,
                    2025-01-15,IWDA,,1.0,1.0,2.0
                    2025-02-01,EUNA.DE,2.0,2.0,4.0,
                    2025-02-01,IWDA,,1.0,1.0,
                    2025-03-01,EUNA.DE,1.0,1.0,3.0,
                    2025-03-01,IWDA,3.0,5.0,11.0,
                    2025-03-15,EUNA.DE,,1.0,3.0,2.0
                    2025-03-15,IWDA,,5.0,11.0,4.0
                    """)),
                     parse_dates=['date'], date_format='%Y-%m-%d')
         .pipe(lambda df: df.assign(date=df['date'].dt.date))),
    )
]

daily_valuations_scenarios = [
    (
        NullQuoteFetcher(),
        (pd.read_csv(StringIO(dedent("""\
                    date,symbol,price,position,invested,close,value,pl
                    2025-01-01,EUNA.DE,,,,,,
                    2025-01-01,IWDA,1.0,1.0,1.0,,1.0,0.0
                    2025-02-01,EUNA.DE,2.0,2.0,4.0,,4.0,0.0
                    2025-02-01,IWDA,,1.0,1.0,,1.0,0.0
                    2025-03-01,EUNA.DE,1.0,1.0,3.0,,1.0,-2.0
                    2025-03-01,IWDA,3.0,5.0,11.0,,15.0,4.0
                    """)),
                     parse_dates=['date'], date_format='%Y-%m-%d')
         .pipe(lambda df: df.assign(date=df['date'].dt.date))),
    ),
    (
        MockQuoteFetcher(),
        (pd.read_csv(StringIO(dedent("""\
                    date,symbol,price,position,invested,close,value,pl
                    2024-12-31,EUNA.DE,,,,,,
                    2024-12-31,IWDA,,,,0.9,,
                    2025-01-01,EUNA.DE,,,,,,
                    2025-01-01,IWDA,1.0,1.0,1.0,1.1,1.0,0.0
                    2025-01-15,EUNA.DE,,,,,,
                    2025-01-15,IWDA,,1.0,1.0,2.0,2.0,1.0
                    2025-02-01,EUNA.DE,2.0,2.0,4.0,,4.0,0.0
                    2025-02-01,IWDA,,1.0,1.0,,2.0,1.0
                    2025-03-01,EUNA.DE,1.0,1.0,3.0,,1.0,-2.0
                    2025-03-01,IWDA,3.0,5.0,11.0,,15.0,4.0
                    2025-03-15,EUNA.DE,,1.0,3.0,2.0,2.0,-1.0
                    2025-03-15,IWDA,,5.0,11.0,4.0,20.0,9.0
                    """)), parse_dates=['date'], date_format='%Y-%m-%d')
         .pipe(lambda df: df.assign(date=df['date'].dt.date))),
    )
]

positions_scenarios = [
    (
        NullQuoteFetcher(),
        (pd.DataFrame({'symbol': ['EUNA.DE', 'IWDA'],
                       'position': [1.0, 5.0],
                       'value': [1.0, 15.0],
                       'invested': [3.0, 11.0],
                       'pl': [-2.0, 4.0]})
         .set_index('symbol'))
    ),
    (
        MockQuoteFetcher(),
        (pd.DataFrame({'symbol': ['EUNA.DE', 'IWDA'],
                       'position': [1.0, 5.0],
                       'value': [2.0, 20.0],
                       'invested': [3.0, 11.0],
                       'pl': [-1.0, 9.0]})
         .set_index('symbol'))
    ),
]


class TestPortfolioPerformance:
    @pytest.fixture
    def transactions(self):
        return pd.DataFrame({
            'trade_date': [datetime(2025, 1, 1, hour=12),
                           datetime(2025, 3, 1, hour=12),
                           datetime(2025, 3, 1, hour=13),
                           datetime(2025, 2, 1),
                           datetime(2025, 3, 1, hour=14)],
            'symbol': ['IWDA', 'EUNA.DE', 'IWDA', 'EUNA.DE', 'IWDA'],
            'side': ['buy', 'sell', 'buy', 'buy', 'buy'],
            'price': [1, 1, 1, 2, 3],
            'quantity': [1, 1, 1, 2, 3]
        })

    @pytest.mark.parametrize("eod_fetcher,expected", daily_positions_scenarios)
    def test_daily_positions(self, transactions, eod_fetcher, expected: pd.DataFrame):
        portfolio = PortfolioPerformance(transactions, eod_fetcher)
        result = portfolio.daily_positions()
        pd.testing.assert_frame_equal(result, expected)

    @pytest.mark.parametrize("eod_fetcher,expected", daily_valuations_scenarios)
    def test_daily_valuations(self, transactions, eod_fetcher, expected):
        portfolio = PortfolioPerformance(transactions, eod_fetcher)
        result = portfolio.daily_valuations()
        pd.testing.assert_frame_equal(result, expected)

    @pytest.mark.parametrize("eod_fetcher,expected", positions_scenarios)
    def test_positions(self, transactions, eod_fetcher, expected):
        portfolio = PortfolioPerformance(transactions, eod_fetcher)
        result = portfolio.positions()
        pd.testing.assert_frame_equal(result, expected)