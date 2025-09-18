from abc import ABC, abstractmethod
from datetime import date

import pandera.pandas as pa
from pandera.typing.pandas import DataFrame


class EODPriceSchema(pa.DataFrameModel):
    date: date = pa.Field(coerce=True)
    symbol: str = pa.Field(coerce=True)
    close: float = pa.Field(coerce=True)


class QuoteFetcher(ABC):
    @abstractmethod
    def get_oed_prices(self, symbol: str, date_from: date, date_to: date) -> DataFrame[EODPriceSchema]:
        raise NotImplementedError


class NullQuoteFetcher(QuoteFetcher):
    @pa.check_types
    def get_oed_prices(self, symbol: str, date_from: date, date_to: date) -> DataFrame[EODPriceSchema]:
        return DataFrame[EODPriceSchema](columns=['date', 'symbol', 'close'])
