""" Demo of parallel queries using ``gsqlite3``. """

from __future__ import print_function
import os
from functools import partial
from random import randint
from timeit import Timer
import gevent
import sqlite3
import gsqlite3
import time
filename = "./geoutils/GN_dump_20160407.sql"
num_rows_to_fetch = 100000


def populate(module, num_rows=1000000):
    pass
    #con = module.connect(filename)
    #con.execute("CREATE TABLE IF NOT EXISTS example (value)")
    #(count,) = con.execute("SELECT count(1) FROM example").fetchone()
    #if count < num_rows:
    #    num_rows -= count
    #    values = [(randint(1, 1000000),) for _ in range(num_rows)]
    #    print("Populating '{}' ...".format(filename))
    #    insert = "INSERT INTO example (value) VALUES (?)"
    #    con.executemany(insert, values)
    #    con.commit()
    #con.close()



def query(conn, lower, upper):
    select_stmt = """SELECT a.id as geonameid, a.name,
               a.population,a.latitude, a.longitude, c.country as 'country',
               b.name as 'admin1',
               a.featureClass, a.countryCode as 'countryCode', a.featureCOde
               FROM allcountries as c
               INNER JOIN allcities as a ON a.countryCode=c.ISO
               LEFT OUTER JOIN alladmins as b ON a.countryCode||'.'||a.admin1 = b.key
               WHERE
               (a.name="{0}" or a.asciiname="{0}") and a.population >= {1}
               """.format(lower, upper)

    #conn = module.connect(filename)
    cur = conn.cursor()
    cur.execute(select_stmt)
    #res = cur.fetchmany(num_rows_to_fetch)
    res = cur.fetchall()
    #conn.close()
    return res
    #con.close()

def query_using_greenlets(module):
    g1 = gevent.spawn(query, module, "buenos aires", 1000)
    g2 = gevent.spawn(query, module, "munnar", 0)
    g3 = gevent.spawn(query, module, "paris", 5000)
    gevent.joinall([g1, g2, g3])
    r = [g1.value, g2.value, g3.value]
    return r

def main():
    modules = [gsqlite3, sqlite3]
    # either module will work here
    populate(modules[0])
    conn = gsqlite3.connect(filename)
    strt = time.time()
    query_using_greenlets(conn)
    end = time.time()
    print(end -strt)

    conn = sqlite3.connect(filename)
    strt = time.time()
    query_using_greenlets(conn)
    end = time.time()
    print(end -strt)
    for module in modules:
        time_query_using_module(module)
    #os.unlink(filename)
    return None


def time_query_using_module(module):
    # This is largely copied verbatim from the 'timeit' module
    repeat = 3
    number = 10
    verbose = True
    precision = 3
    conn = module.connect(filename)
    stmt = partial(query_using_greenlets, conn)
    t = Timer(stmt)
    try:
        r = t.repeat(repeat, number)
    except:
        print("error")
        t.print_exc()
        return 1
    best = min(r)
    if verbose:
        print("raw times:", " ".join(["%.*g" % (precision, x) for x in r]))
    print("%s: %d loops," % (module.__name__, number))
    usec = best * 1e6 / number
    if usec < 1000:
        print("best of %d: %.*g usec per loop" % (repeat, precision, usec))
    else:
        msec = usec / 1000
        if msec < 1000:
            print("best of %d: %.*g msec per loop" % (repeat, precision, msec))
        else:
            sec = msec / 1000
            print("best of %d: %.*g sec per loop" % (repeat, precision, sec))

if __name__ == '__main__':
    main()
