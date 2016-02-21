#! /usr/bin/python
"""
Load stock quote data for tickers in database.
"""

import sys
import threading
import Queue
import time
import psycopg2
import logging
import logging.handlers
import random

from yahoo_finance import Share

__author__ = 'hhemken'
log = logging.getLogger(__file__)
# Add the log message handler to the logger
LOG_FILENAME = 'quote_data_daemon.log'
LOG_MAX_SIZE = 50 * 1024 * 1024
LOG_MAX_BACKUPS = 25
handler = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=LOG_MAX_SIZE, backupCount=LOG_MAX_BACKUPS)
log.addHandler(handler)


class QuoteDataThread(threading.Thread):
    def __init__(self, msg_queue):
        threading.Thread.__init__(self)
        self.msg_queue = msg_queue
        self.rng = random.SystemRandom()

    def _connect(self):
        """
        Connect to database and create db cursor
        :return:
        """
        database = 'trade_forecast'
        user = 'trade_forecast'
        password = '867-5309'

        try:
            self.pg_conn = psycopg2.connect("dbname='%s' user='%s' host='localhost' password='%s'" %
                                            (database, user, password))
        except:
            raise Exception("Unable to connect to database %s" % database)
        log.info("connected to database %s", database)
        self.pg_cursor = self.pg_conn.cursor()

    def run(self):
        """
        start the thread's activities
        :return:
        """
        try:
            self._connect()
            for exchange_name in self.rng.shuffle(['NYSE', 'AMEX', 'NASDAQ']):
                self.load_quote_data(exchange_name)
            #            raise Exception('An error occurred here.')
        except Exception:
            self.msg_queue.put(sys.exc_info())

    def get_daily_price_count(self, symbol_id):
        """
        see how many quote days are in the db for this symbol_id
        :param symbol_id:
        :return:
        """
        self.pg_cursor.execute("select count(*) from t_daily_price where symbol_id = %d;" % symbol_id)
        daily_price_count = self.pg_cursor.fetchone()[0]
        log.info('symbol_id %d has %d daily price quotes', symbol_id, daily_price_count)
        assert isinstance(daily_price_count, int)
        return daily_price_count

    def load_quote_data(self, exchange):
        """
        go through all of the tickers in the db associated with this exchange and download all of their historical data
        :param exchange:
        :return:
        """
        today = time.strftime("%Y-%m-%d")
        self.pg_cursor.execute("SELECT id FROM t_exchange WHERE abbrev = '%s';" % exchange)
        exchange_id = self.pg_cursor.fetchone()[0]
        log.info('id of %s is %d', exchange, exchange_id)
        self.pg_cursor.execute("SELECT ticker FROM t_symbol WHERE exchange_id = %d;" % exchange_id)
        rows = self.pg_cursor.fetchall()
        for row in self.rng.shuffle(rows):
            ticker = row[0]
            log.info('ticker: "%s"', ticker)
            ticker_quotes = Share(ticker)
            self.pg_cursor.execute("SELECT id FROM t_symbol WHERE ticker = '%s' AND exchange_id = %d;" %
                                   (ticker, exchange_id))
            symbol_id = self.pg_cursor.fetchone()[0]
            log.info('id of %s is %d', ticker, symbol_id)
            daily_price_count = self.get_daily_price_count(symbol_id)
            if daily_price_count > 0:
                log.info('skipping %s since it already has %d quotes', ticker, daily_price_count)
                continue
            historical_data = ticker_quotes.get_historical('1900-01-01', today)
            log.info('    got %d quotes for %s', len(historical_data), ticker)
            # [{'Volume': '17600', 'Symbol': 'AAMC', 'Adj_Close': '16.030001', 'High': '17.91', 'Low': '15.95',
            #   'Date': '2016-02-05', 'Close': '16.030001', 'Open': '17.459999'},
            #  {'Volume': '7800', 'Symbol': 'AAMC', 'Adj_Close': '16.99', 'High': '17.290001', 'Low': '15.59',
            #   'Date': '2016-02-04', 'Close': '16.99', 'Open': '15.59'},
            # ...]
            cnt = 0
            failed = False
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
                    sql_cmd = "SELECT f_download_results_failed(%s);" % (str(symbol_id))
                    log.info('bad data received for %s, skipping and calling "%s"', ticker, sql_cmd)
                    self.pg_cursor.execute(sql_cmd)
                    failed = True
                    continue
                if cnt % 100 == 0:
                    log.info('        processed %d out of %d %s quotes so far', cnt, len(historical_data), ticker)
                pct_change = (float(quote['Close']) - float(quote['Open'])) / float(quote['Open'])
                self.pg_cursor.execute("SELECT * FROM f_daily_price_ins_upd(%d, '%s', %s, %s, %s, %s, %s, %s, %s);" %
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
            if not failed:
                if cnt > 0:
                    sql_cmd = "SELECT f_download_results_succeeded(%s);" % (str(symbol_id))
                    log.info('successfully downloaded data for %s, calling "%s"', ticker, sql_cmd)
                    self.pg_cursor.execute(sql_cmd)
                elif cnt == 0:
                    sql_cmd = "SELECT f_download_results_failed(%s);" % (str(symbol_id))
                    log.info('no data received for %s, calling "%s"', ticker, sql_cmd)
                    self.pg_cursor.execute(sql_cmd)
            self.pg_conn.commit()
            log.info('sleep 6 s')
            time.sleep(6)


if __name__ == '__main__':
    quote_data_log_format = ("%(asctime)s.%(msecs)03d %(levelname)-06s: " +
                             "%(module)s::%(funcName)s:%(lineno)s: %(message)s")
    quote_data_log_datefmt = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(level=logging.DEBUG,
                        format=quote_data_log_format,
                        datefmt=quote_data_log_datefmt)

    main_msg_queue = Queue.Queue()
    thread_obj = QuoteDataThread(main_msg_queue)
    thread_obj.start()

    while True:
        try:
            exc = main_msg_queue.get(block=False)
        except Queue.Empty:
            pass
        else:
            exc_type, exc_obj, exc_trace = exc
            # deal with the exception
            log.exception("thread exception type %s: %s", str(exc_type), str(exc_obj))
            log.exception("thread exception trace: %s", str(exc_trace))

        thread_obj.join(0.1)
        if thread_obj.isAlive():
            continue
        else:
            log.warn('thread was dead, restarting!')
            thread_obj = QuoteDataThread(main_msg_queue)
            thread_obj.start()
# break
