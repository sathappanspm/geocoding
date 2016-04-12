#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

import unicodecsv
import sqlite3
import sys
# from time import sleep
from . import GeoPoint

unicodecsv.field_size_limit(sys.maxsize)

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"


class BaseDB(object):
    def __init__(self, dbpath):
        pass

    def insert(self, keys, values, **args):
        pass


class SQLiteWrapper(BaseDB):
    def __init__(self, dbpath, dbname="WorldGazetteer"):
        self.conn = sqlite3.connect(dbpath)
        self.conn.execute('PRAGMA synchronous = OFF')
        self.conn.execute('PRAGMA journal_mode = WAL')
        self.conn.execute("PRAGMA cache_size=2000000")

        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.name = dbname

    def query(self, stmt=None, items=[]):
        result = self.cursor.execute(stmt)
        return [GeoPoint(**dict(l)) for l in result.fetchall()]

    def insert(self, msg, dup_id):
        return

    def _create(self, csvPath, columns, delimiter="\t", coding='utf-8', **kwargs):
        with open(csvPath, "rU") as infile:
 #           dialect = unicodecsv.Sniffer().sniff(infile.read(10240),
                                                 #delimiters=delimiter)
            infile.seek(0)
            reader = unicodecsv.DictReader(infile, dialect='excel',
                                           fieldnames=columns, encoding=coding)
            items = [r for r in reader]
            self.cursor.execute('''CREATE TABLE WorldGazetteer (id float,
                                    name text, alt_names text, orig_names text, type text,
                                    pop integer, latitude float, longitude float, country text,
                                    admin1 text, admin2 text, admin3 text)''')
            self.cursor.executemany('''INSERT INTO WorldGazetteer
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                    [tuple([i[c] for c in columns]) for i in items])
            self.conn.commit()

    def create(self, csvfile, fmode='r', delimiter='\t',
               coding='utf-8', columns=[], header=False, **kwargs):
        with open(csvfile, fmode) as infile:
 #           dialect = unicodecsv.Sniffer().sniff(infile.read(10240),
 #                                                delimiters=delimiter)
            infile.seek(0)
            if header:
                try:
                    splits = infile.next().split(delimiter)
                    if not columns:
                        columns = splits
                except Exception:
                    pass

            #reader = unicodecsv.DictReader(infile, dialect='excel',
            #                               fieldnames=columns, encoding=coding)
            self.cursor.execute(''' CREATE TABLE IF NOT EXISTS {} ({})'''.format(self.name,
                                                                                 ','.join(columns)))

            reader = infile
            columnstr = ','.join(['?' for c in columns])

            self.cursor.executemany('''INSERT INTO {}
                                    VALUES ({})'''.format(self.name, columnstr),
                                    (tuple([i for i in c.decode("utf-8").split("\t")]) for c in reader))
                                    #(tuple([c[i] for i in columns]) for c in reader))
            self.conn.commit()
            if 'index' in kwargs:
                for i in kwargs['index']:
                    col, iname = i.split()
                    self.cursor.execute('CREATE INDEX IF NOT EXISTS {} ON {} ({})'.format(iname.strip(),
                                                                                          self.name,
                                                                                          col.strip()))
                    self.conn.commit()
            self.conn.commit()

    def close(self):
        self.conn.commit()

    def drop(self, tablename):
        self.cursor.execute("DROP TABLE IF EXISTS {}".format(tablename))
        self.conn.commit()


def main(args):
    """
    """
    import json
    with open(args.config) as cfile:
        config = json.load(cfile)

    fmode = config['mode']
    separator = config['separator']
    columns = config['columns']
    coding = config['coding']
    dbname = args.name
    infile = args.file
    index = config['index']
    sql = SQLiteWrapper(dbpath="./GN_dump_20160407.sql", dbname=dbname)
    if args.overwrite:
        sql.drop(dbname)
    sql.create(infile, mode=fmode, columns=columns,
               delimiter=separator, coding=coding, index=index)

    sql.close()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--name", type=str, help="database name")
    ap.add_argument("-c", "--config", type=str, help="config file containing database info")
    ap.add_argument("-f", "--file", type=str, help="csv name")
    ap.add_argument("-o", "--overwrite", action="store_true",
                    default=False, help="flag to overwrite if already exists")
    args = ap.parse_args()
    main(args)
