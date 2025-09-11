from datetime import date
from functools import reduce

import pandas as pd

from gainly.eod import QuoteFetcher, NullQuoteFetcher


class PortfolioPerformance(object):
    """Calculates portfolio performance."""

    def __init__(self, transactions: pd.DataFrame, quote_fetcher: QuoteFetcher = None):
        """
        :param transactions:    a DataFrame of transactions, with columns:
                                trade_date, symbol, side, price, quantity
        :param quote_fetcher:   a QuoteFetcher instance for historical end-of-day price data
        """
        self.txns = transactions.copy()
        quote_fetcher = quote_fetcher or NullQuoteFetcher()
        self.eod_prices = reduce(
            lambda x, y: pd.concat([x, y]),
            (quote_fetcher.get_oed_prices(symbol,
                                          date_from=self.txns['trade_date'].min().date(),
                                          date_to=max(self.txns['trade_date'].max().date(), date.today()))
             for symbol in self.txns['symbol'].unique()))

    def daily_positions(self) -> pd.DataFrame:
        """Returns a DataFrame of daily positions for the portfolio."""
        df = self.txns.copy().set_index('trade_date').sort_index()
        df['position'] = (df.assign(quantity=df['quantity'] * df['side'].map({'buy': 1, 'sell': -1}))
                            .groupby('symbol')['quantity']
                            .cumsum())

        # Convert datetime index to date
        df.index = df.index.date

        # Get end-of-day positions and values for each symbol
        daily_positions = (df[['symbol', 'price', 'position']]
                           .groupby([df.index, 'symbol'])
                           .last()
                           .reset_index(names=['date', 'symbol']))

        # Merge the eod prices with the daily positions, excluding eod prices for dates where we already have a price
        # from a transaction:
        daily_positions = daily_positions.merge(self.eod_prices, how='outer', on=['date', 'symbol'])

        # Create the cartesian product grid of all trade dates and symbols:
        grid = pd.DataFrame([(date, symbol)
                             for date in daily_positions['date'].unique()
                             for symbol in daily_positions['symbol'].unique()],
                            columns=['date', 'symbol'])
        # Merge grid with positions data so that we can generate a position for each day and symbol
        daily_positions = daily_positions.merge(grid, how='outer')

        # On dates where we don't have a trade for all symbols, fill the position with the previous day's position
        # for each symbol:
        daily_positions['position'] = daily_positions.groupby('symbol')['position'].ffill()
        return daily_positions

    def daily_valuations(self) -> pd.DataFrame:
        """Returns a DataFrame with the day-to-day total value of the portfolio."""
        daily_positions = self.daily_positions()
        # Now that we have a position for each symbol on each day, we can calculate the total value of the portfolio
        # by multiplying the position by "price", or "close":
        daily_positions['value'] = (daily_positions['position'] *
                                    daily_positions['price'].combine_first(daily_positions['close']))
        # For days when we can't calculate the value of a symbol's position due to lack of both a trade price and an
        # EOD price, carry forward the previous day's value for that symbol:
        daily_positions['value'] = daily_positions.groupby('symbol')['value'].ffill()

        return daily_positions

    def positions(self) -> pd.DataFrame:
        """Returns the portfolio's current positions along with the current market value for each position."""
        return self.daily_valuations().groupby('symbol')[['position', 'value']].last()
