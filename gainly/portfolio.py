import pandas as pd


class PortfolioPerformance(object):
    """Calculates portfolio performance."""
    _empty_eods = pd.DataFrame(columns=['date', 'symbol', 'close']).astype({
        'date': 'datetime64[ns]',
        'symbol': str,
        'close': float
    }).pipe(lambda df: df.assign(date=df['date'].dt.date))

    def __init__(self, transactions: pd.DataFrame):
        self.txns = transactions.copy()

    def daily_positions(self, eod_prices: pd.DataFrame = None) -> pd.DataFrame:
        """Returns a DataFrame of daily positions for the portfolio.

        :param eod_prices:  A DataFrame of end-of-day prices for the symbols in the portfolio.
                            The DataFrame's `date` column should be of type `date` (not `datetime`).
        """
        df = self.txns.copy().set_index('trade_date').sort_index()
        df['position'] = (df.assign(quantity=df['quantity'] * df['side'].map({'buy': 1, 'sell': -1}))
                            .groupby('symbol')['quantity']
                            .cumsum())

        # Convert datetime index to date
        df.index = df.index.date
        # print(df)

        # Get end-of-day positions and values for each symbol
        daily_positions = df[['symbol', 'price', 'position']].groupby([df.index, 'symbol']).last().reset_index(
            names=['date', 'symbol'])
        # print(daily_positions)

        # Merge the eod prices with the daily positions, excluding eod prices for dates where we already have a price
        # from a transaction:
        daily_positions = daily_positions.merge(self._empty_eods if eod_prices is None else eod_prices,
                                                how='outer', on=['date', 'symbol'])

        # Create the cartesian product grid of all trade dates and symbols:
        grid = pd.DataFrame([(date, symbol)
                             for date in daily_positions['date'].unique()
                             for symbol in daily_positions['symbol'].unique()], columns=['date', 'symbol'])
        # Merge grid with positions data so that we can generate a position for each day and symbol
        daily_positions = daily_positions.merge(grid, how='outer', )
        # print(daily_positions)

        # On dates where we don't have a trade for all symbols, fill the position with the previous day's position
        # for each symbol:
        daily_positions['position'] = daily_positions.groupby('symbol')['position'].ffill()
        # print(daily_positions)

        return daily_positions

    def daily_valuations(self, eod_prices: pd.DataFrame = None) -> pd.DataFrame:
        """Returns a DataFrame with the day-to-day total value of the portfolio."""
        daily_positions = self.daily_positions(eod_prices)
        # Now that we have a position for each symbol on each day, we can calculate the total value of the portfolio
        # by multiplying the position by "price", or "close":
        daily_positions['value'] = (daily_positions['position'] *
                                    daily_positions['price'].combine_first(daily_positions['close']))
        # For days when we can't calculate the value of a symbol's position due to lack of both a trade price and an
        # EOD price, carry forward the previous day's value for that symbol:
        daily_positions['value'] = daily_positions.groupby('symbol')['value'].ffill()
        # print(daily_positions)

        return daily_positions

    def positions(self, eod_prices: pd.DataFrame = None) -> pd.DataFrame:
        """Returns the portfolio's current positions along with the current market value for each position."""
        return self.daily_valuations(eod_prices).groupby('symbol')[['position', 'value']].last()
