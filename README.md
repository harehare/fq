fq
====================

SQL query for tsv, csv and json

fq is a simple tool to operate csv, tsv and json files. You can join records with SQL query.

## Install

```
$ pip install git+ssh://git@github.com/harehare/fq.git
```

## Usage

```
fq.

Usage:
  fq.py (-q <query>) (-f <output-format>) [<files>...]
  fq.py (-h | --help)
  fq.py --version

Options:
  -h --help            Show this screen.
  --version            Show version.
  -q <query>           Input SQL query from raw string.
  -f <output-format>   Output file format one of 'jsonl', 'csv', 'tsv'.
```

## Example

Table names are the basenames of the files without extension.

```bash
$ fq -q "SELECT * FROM test LEFT JOIN test2 ON test.id = test2.id LEFT JOIN test3 ON test.id = test3.id" -o tsv test.json test2.tsv.gz http://localhost/test3.json
```
