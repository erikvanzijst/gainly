# Gainly: Simple portfolio tracker

Very simple app to track portfolio performance based on a ledger of buy/sell transactions.

Useful for keeping a single view of a portfolio that might be scattered across multiple brokers.

## Usage

```
PYTHONPATH=. uv run streamlit run gainly/lit/main.py
```


## Tests

```
uv run pytest -s tests/
```


# TODO

- [X] Support multiple transactions on the same timestamp
- [ ] Compute capital gains for all sell orders and display in txn history
- [ ] Multi currency support
- [ ] Compute cost base per position
- [ ] Create a normalized instrument table with isin/yahoo_ticker/asset_class/currency
