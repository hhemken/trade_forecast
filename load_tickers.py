#! /usr/bin/python
"""
Load csv files with tickers obtained from:
http://www.nasdaq.com/screening/company-list.aspx

Files include NYSE, NASDAQ, and AMEX
The csv files may have extraneous commas at the end of each line that will produce phantom key/value pairs
"""

import csv
import psycopg2
import logging

__author__ = 'hhemken'
log = logging.getLogger(__file__)


def load_tickers(pg_cursor, filename, exchange):
    f = open(filename, 'rt')
    try:
        reader = csv.DictReader(f)
        for row in reader:
            # f_symbol_ins_upd(ticker_i, company_name_i, sector_i, industry_i, exchange_i)
            pg_cursor.execute("SELECT * FROM f_symbol_ins_upd('%s', '%s', '%s', '%s', '%s');" %
                              (row['Symbol'].strip(),
                               row['Name'].strip(),
                               row['Sector'].strip(),
                               row['industry'].strip(),
                               exchange))
            log.info('stored row "%s" in database', str(row))
    except Exception, ex_obj:
        raise Exception("error writing to database: %s" % str(ex_obj))
    finally:
        f.close()


if __name__ == '__main__':
    tintri_log_format = ("%(asctime)s.%(msecs)03d [%(process)d] %(threadName)s: %(levelname)-06s: " +
                         "%(module)s::%(funcName)s:%(lineno)s: %(message)s")
    tintri_log_datefmt = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(level=logging.DEBUG,
                        format=tintri_log_format,
                        datefmt=tintri_log_datefmt)

    database = 'trade_forecast'
    user = 'trade_forecast'
    password = '867-5309'

    try:
        conn = psycopg2.connect("dbname='%s' user='%s' host='localhost' password='%s'" %
                                (database, user, password))
    except:
        raise Exception("Unable to connect to database %s" % database)
    log.info("connected to database %s", database)
    psyco_cur = conn.cursor()

    for ticker_file, exchange_name in [('data/amex-companylist-2016-02-05.csv', 'AMEX'),
                                       ('data/nasdaq-companylist-2016-02-05.csv', 'NASDAQ'),
                                       ('data/nyse-companylist-2016-02-05.csv', 'NYSE')
                                       ]:
        load_tickers(psyco_cur, ticker_file, exchange_name)
        conn.commit()
