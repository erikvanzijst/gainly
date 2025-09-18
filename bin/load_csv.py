import argparse
import sqlite3

import pandas as pd
from pandera.typing.pandas import DataFrame

from gainly.portfolio import TransactionSchema


def main():
    parser = argparse.ArgumentParser(description='Load csv files contain trade reports into the database.')
    parser.add_argument('--db', help='the target sqlite3 database file', default='db/transactions.db')
    parser.add_argument('csv', help='a local csv file')
    args = parser.parse_args()

    # Load the csv file and validate its contents against the TransactionSchema:
    df = (pd.read_csv(args.csv, parse_dates=['trade_date'], date_format='%Y-%m-%d %H:%M:%S')
            .pipe(DataFrame[TransactionSchema]))

    with sqlite3.connect(args.db) as conn:
        num = df.to_sql('txn', conn, if_exists='append', index=False)
        print(f'Inserted {num} rows.')


if __name__ == '__main__':
    main()
