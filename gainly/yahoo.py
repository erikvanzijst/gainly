import urllib
from datetime import date, datetime, timezone, time, timedelta
from functools import cache

import pandas as pd
import requests

from gainly.eod import QuoteFetcher


@cache
def make_request(url: str) -> dict:
    res = requests.get(url, headers={'User-Agent': 'Prutser'})
    res.raise_for_status()
    return res.json()


class YahooFinance(QuoteFetcher):
    def __init__(self):
        pass

    def get_oed_prices(self, symbol: str, date_from: date, date_to: date) -> pd.DataFrame:
        qs = urllib.parse.urlencode(
            {'interval': '1d',
             'period1': to_epoch(date_from),
             'period2': to_epoch(date_to),})

        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?' + qs
        payload: dict = make_request(url)

        tz = timezone(timedelta(seconds=payload['chart']['result'][0]['meta']['gmtoffset']))
        timestamps = [datetime.fromtimestamp(ts, tz).date() for ts in payload['chart']['result'][0]['timestamp']]
        closes = payload['chart']['result'][0]['indicators']['quote'][0]['close']

        return pd.DataFrame({'date': timestamps, 'close': closes}).assign(symbol=symbol)


def to_epoch(dt: datetime|date) -> int:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    else:
        return int(datetime.combine(dt, time(hour=23, minute=59, second=59), tzinfo=timezone.utc).timestamp())
