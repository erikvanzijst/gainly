from datetime import date, datetime
from functools import reduce
from itertools import product

import pandas as pd
import pandera.pandas as pa
from pandera.typing.pandas import Index, DataFrame

from gainly.eod import QuoteFetcher, NullQuoteFetcher


class TransactionSchema(pa.DataFrameModel):
    trade_date: datetime = pa.Field(coerce=True)
    symbol: str = pa.Field(coerce=True)
    price: float = pa.Field(ge=0, coerce=True)
    quantity: float = pa.Field(coerce=True)


class DailyPositionsSchema(pa.DataFrameModel):
    date: date = pa.Field(coerce=True)
    symbol: str = pa.Field(coerce=True)
    price: float = pa.Field(ge=0, nullable=True, coerce=True)
    position: float = pa.Field(ge=0, nullable=True, coerce=True)
    invested: float = pa.Field(nullable=True, coerce=True)


class DailyValuationSchema(DailyPositionsSchema):
    close:float = pa.Field(nullable=True, coerce=True)
    value: float = pa.Field(nullable=True, coerce=True)
    pl: float = pa.Field(nullable=True, coerce=True)


class PositionsSchema(pa.DataFrameModel):
    symbol: Index[str] = pa.Field(coerce=True)
    position: float = pa.Field(ge=0, coerce=True)
    value: float = pa.Field(coerce=True)
    invested: float = pa.Field(coerce=True)
    pl: float = pa.Field(coerce=True)


class PortfolioPerformance(object):
    """Calculates portfolio performance."""

    @pa.check_types
    def __init__(self, transactions: DataFrame[TransactionSchema], quote_fetcher: QuoteFetcher = None):
        """
        :param transactions:    a DataFrame of transactions, with columns:
                                trade_date, symbol, price, quantity
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

    @pa.check_types
    def daily_positions(self) -> DataFrame[DailyPositionsSchema]:
        """Returns a DataFrame of daily positions for the portfolio."""
        df = self.txns.copy().set_index('trade_date').sort_index()
        df['position'] = (df.groupby('symbol')['quantity']
                            .cumsum())
        df['invested'] = (df.assign(invested=df['price'] * df['quantity'])
                          .groupby('symbol')['invested']
                          .cumsum())

        # Convert datetime index to date
        df.index = df.index.date

        # Get end-of-day positions, price and total invested amount per symbol
        daily_positions = (df[['symbol', 'price', 'position', 'invested']]
                           .groupby([df.index, 'symbol'])
                           .last()
                           .reset_index(names=['date', 'symbol']))

        # Merge the eod prices with the daily positions, excluding eod prices for dates where we already have a price
        # from a transaction:
        daily_positions = daily_positions.merge(self.eod_prices, how='outer', on=['date', 'symbol'])

        # Create the cartesian product grid of all trade dates and symbols:
        grid = pd.DataFrame(list(product(daily_positions['date'].unique(), daily_positions['symbol'].unique())),
                            columns=['date', 'symbol'])
        # Merge grid with positions data so that we can generate a position for each day and symbol
        daily_positions = daily_positions.merge(grid, how='outer')

        # On dates where we don't have a trade for all symbols, fill the position with the previous day's position
        # for each symbol:
        daily_positions['position'] = daily_positions.groupby('symbol')['position'].ffill()
        daily_positions['invested'] = daily_positions.groupby('symbol')['invested'].ffill()
        return daily_positions

    @pa.check_types
    def daily_valuations(self) -> DataFrame[DailyValuationSchema]:
        """Returns a DataFrame with the day-to-day total value of the portfolio."""
        daily_positions = self.daily_positions()

        # Now that we have a position for each symbol on each day, we can calculate the total value of the portfolio
        # by multiplying the position by "price", or "close":
        daily_positions['value'] = (daily_positions['position'] *
                                    daily_positions['price'].combine_first(daily_positions['close']))
        daily_positions['pl'] = daily_positions['value'] - daily_positions['invested']

        # For days when we can't calculate the value of a symbol's position due to lack of both a trade price and an
        # EOD price, carry forward the previous day's value for that symbol:
        daily_positions['value'] = daily_positions.groupby('symbol')['value'].ffill()
        daily_positions['pl'] = daily_positions.groupby('symbol')['pl'].ffill()

        return daily_positions.pipe(DataFrame[DailyValuationSchema])

    @pa.check_types
    def positions(self) -> DataFrame[PositionsSchema]:
        """Returns the portfolio's current positions along with the current market value for each position.

        :return:    a DataFrame adhering to the `positionsSchema` schema.
        """
        return (self
                .daily_valuations()
                .groupby('symbol')[['position', 'value', 'invested', 'pl']]
                .last())
