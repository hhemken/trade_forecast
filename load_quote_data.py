#! /usr/bin/python
"""
Load stock quote data for tickers in database.
"""

import time
import psycopg2
import logging
from yahoo_finance import Share

__author__ = 'hhemken'
log = logging.getLogger(__file__)


def get_daily_price_count(pg_cursor, symbol_id):
    """

    :param symbol_id:
    :return:
    """
    pg_cursor.execute("select count(*) from t_daily_price where symbol_id = %d;;" % symbol_id)
    daily_price_count = pg_cursor.fetchone()[0]
    log.info('symbol_id has %d daily price quotes', daily_price_count)
    return daily_price_count


def load_quote_data(pg_conn, pg_cursor, exchange):
    """
    :param pg_cursor:
    :param exchange:
    :return:
    """
    today = time.strftime("%Y-%m-%d")
    pg_cursor.execute("SELECT id FROM t_exchange WHERE abbrev = '%s';" % exchange)
    exchange_id = pg_cursor.fetchone()[0]
    log.info('id of %s is %d', exchange, exchange_id)
    pg_cursor.execute("SELECT ticker FROM t_symbol WHERE exchange_id = %d;" % exchange_id)
    rows = pg_cursor.fetchall()
    for row in sorted(rows):
        ticker = row[0]
        log.info('ticker: "%s"', ticker)
        ticker_quotes = Share(ticker)
        pg_cursor.execute("SELECT id FROM t_symbol WHERE ticker = '%s' AND exchange_id = %d;" %
                          (ticker, exchange_id))
        symbol_id = pg_cursor.fetchone()[0]
        log.info('id of %s is %d', ticker, symbol_id)
        daily_price_count = get_daily_price_count(pg_cursor, symbol_id)
        if daily_price_count > 0:
            log.info('skipping %s since it already has %d quotes', ticker, daily_price_count)
            continue
        historical_data = ticker_quotes.get_historical('1900-01-01', today)
        log.info('    got %d quotes for %s', len(historical_data), ticker)
        # [{'Volume': '17600', 'Symbol': 'AAMC', 'Adj_Close': '16.030001', 'High': '17.91', 'Low': '15.95', 'Date': '2016-02-05', 'Close': '16.030001', 'Open': '17.459999'},
        #  {'Volume': '7800', 'Symbol': 'AAMC', 'Adj_Close': '16.99', 'High': '17.290001', 'Low': '15.59', 'Date': '2016-02-04', 'Close': '16.99', 'Open': '15.59'},
        # ...]
        cnt = 0
        for quote in historical_data:
            # CREATE OR REPLACE FUNCTION f_daily_price_ins_upd(
            #     symbol_id_i         BIGINT,
            #     price_date_i        TIMESTAMP,
            #     open_price_i        numeric(8,6),
            #     high_price_i        numeric(8,6),
            #     low_price_i         numeric(8,6),
            #     close_price_i       numeric(8,6),
            #     adj_close_price_i   numeric(8,6),
            #     percent_change_i    numeric(8,6),
            #     volume_i            BIGINT)
            if 'Date' not in quote:
                log.info('bad data received for %s, skipping', ticker)
                continue
            if cnt % 100 == 0:
                log.info('        processed %d out of %d %s quotes so far', cnt, len(historical_data), ticker)
            pct_change = (float(quote['Close']) - float(quote['Open'])) / float(quote['Open'])
            pg_cursor.execute("SELECT * FROM f_daily_price_ins_upd(%d, '%s', %s, %s, %s, %s, %s, %s, %s);" %
                              (symbol_id,
                               quote['Date'].strip(),
                               quote['Open'].strip(),
                               quote['High'].strip(),
                               quote['Low'].strip(),
                               quote['Close'].strip(),
                               quote['Adj_Close'].strip(),
                               pct_change,
                               quote['Volume'].strip()))
            cnt += 1
        pg_conn.commit()
        log.info('sleep 10 s')
        time.sleep(10)

if __name__ == '__main__':
    tintri_log_format = ("%(asctime)s.%(msecs)03d %(levelname)-06s: " +
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

    for exchange_name in ['NYSE', 'AMEX']:
        load_quote_data(conn, psyco_cur, exchange_name)


