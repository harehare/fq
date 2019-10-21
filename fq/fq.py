"""fq.

Usage:
  fq.py (-q <query>) (-o <output-format>) [<files>...]
  fq.py (-h | --help)
  fq.py --version

Options:
  -h --help            Show this screen.
  --version            Show version.
  -q <query>           Input sql query from raw string.
  -o <output-format>   Output file format one of 'json', 'jsonl', 'csv', 'tsv'.
"""
from docopt import docopt
import sys
import os
import csv
import json
import pathlib
import sqlite3
import tempfile
import pandas as pd
import time
from typing import Dict, List
import traceback

__version__ = '0.1.0'

VALID_EXTENSION = [
    '.json', '.jsonl', '.csv', '.tsv', '.gz', '.bz2', '.zip', '.xz'
]


def file_to_df(file_name: str) -> (List[str], pd.DataFrame):
    ext = pathlib.PurePosixPath(file_name).suffix

    if ext not in VALID_EXTENSION:
        raise ValueError(f'{ext} is not supported file type.')

    if not os.path.exists(file_name):
        raise ValueError(f'{file_name} is not exists.')

    if ext == '.json' or file_name.endswith('.json.gz'):
        df = pd.read_json(file_name)
        for column in df.columns:
            df[column] = df[column].astype(str)
        return df.columns.values.tolist(), df
    elif ext == '.jsonl' or file_name.endswith('.jsonl.gz'):
        df = pd.read_json(file_name, orient='records', lines=True)
        for column in df.columns:
            df[column] = df[column].astype(str)
        return df.columns.values.tolist(), df
    elif ext == '.tsv' or file_name.endswith('.tsv.gz'):
        df = pd.read_table(file_name,
                           error_bad_lines=False,
                           warn_bad_lines=False)
        return df.columns.values.tolist(), df
    elif ext == '.csv' or file_name.endswith('.csv.gz'):
        df = pd.read_csv(file_name,
                         error_bad_lines=False,
                         warn_bad_lines=False)
        return df.columns.values.tolist(), df
    else:
        raise ValueError(f'{ext} is not supported file type.')


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect('.fq_cache.db')
    conn.row_factory = sqlite3.Row
    return conn


def import_file(conn: sqlite3.Connection, table_name: str,
                column_names: List[str], df: pd.DataFrame):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} {'(' + ','.join([f'{col} TEXT' for col in column_names]) + ')'}"
    )
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    cursor.close()


def execute_query(conn: sqlite3.Connection, query: str) -> sqlite3.Row:
    cursor = conn.cursor()
    return cursor.execute(query).fetchall()


def print_results(rows: sqlite3.Row, ext: str):
    if ext == '.jsonl':
        for row in rows:
            print(json.dumps({key: row[key] for key in row.keys()}))
        return
    elif ext == '.json':
        print(json.dumps([dict(row) for row in rows]))
        return

    delimiter = '\t'

    if ext == '.csv':
        delimiter = ','

    column_names = None
    for row in rows:
        if not column_names:
            column_names = row.keys()
            print(delimiter.join(column_names))
            continue
        print(
            delimiter.join([
                str(row[name]) if row[name] else '' for name in column_names
            ]))


def main():
    start = time.time()
    args = docopt(__doc__, version="{0}".format('0.1.0'))
    conn = open_db()
    try:
        if f".{args['-o']}" not in VALID_EXTENSION:
            sys.stderr.write(f"{args['-o']} is Invalid file type.\n")
            sys.exit(1)

        for f in args['<files>']:
            table_name = pathlib.Path(f).stem.split('.')[0]
            columns, df = file_to_df(f)
            import_file(conn, table_name, columns, df)

        rows = execute_query(conn, args['-q'])
        print_results(rows, f".{args['-o']}")
        sys.stderr.writelines(
            [f'{len(rows)} rows selected ({round(time.time() - start, 2)} seconds)'])

        conn.close()
    except sqlite3.OperationalError as e:
        sys.stderr.writelines([str(e)])
    except Exception as e:
        traceback.print_exc()
        sys.stderr.writelines([f'Internal error. cause {str(e)}'])
    finally:
        conn.close()


if __name__ == '__main__':
    main()
