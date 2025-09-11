from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class QuoteFetcher(ABC):
    @abstractmethod
    def get_oed_prices(self, symbol: str, date_from: date, date_to: date) -> pd.DataFrame:
        raise NotImplementedError


class NullQuoteFetcher(QuoteFetcher):
    def get_oed_prices(self, symbol: str, date_from: date, date_to: date) -> pd.DataFrame:
        return pd.DataFrame(columns=['date', 'symbol', 'close']).astype({
            'date': 'datetime64[ns]',
            'symbol': str,
            'close': float
        }).pipe(lambda df: df.assign(date=df['date'].dt.date))
