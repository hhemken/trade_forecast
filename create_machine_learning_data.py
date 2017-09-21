#! /usr/bin/python3
"""
Load stock quote data for tickers in database.
"""

import sys
import psycopg2
import logging
import argparse
from random import SystemRandom

__author__ = 'hhemken'
log = logging.getLogger(__file__)

KEY_STRIDES = 'strides'
KEY_WINDOW = 'window'

QUOTE_WINDOWS = {
    4: {KEY_STRIDES: {1: 4, 2: 2, 4: 1}},
    8: {KEY_STRIDES: {1: 8, 2: 4, 4: 2, 8: 1}},
    16: {KEY_STRIDES: {1: 16, 2: 8, 4: 4, 8: 2, 16: 1}},
    32: {KEY_STRIDES: {1: 32, 2: 16, 4: 8, 8: 4, 16: 2, 32: 1}},
    64: {KEY_STRIDES: {1: 64, 2: 32, 4: 16, 8: 8, 16: 4, 32: 2, 64: 1}},
    128: {KEY_STRIDES: {1: 128, 2: 64, 4: 32, 8: 16, 16: 8, 32: 4, 64: 2, 128: 1}},
    256: {KEY_STRIDES: {1: 256, 2: 128, 4: 64, 8: 32, 16: 16, 32: 8, 64: 4, 128: 2, 256: 1}},
    512: {KEY_STRIDES: {1: 512, 2: 256, 4: 128, 8: 64, 16: 32, 32: 16, 64: 8, 128: 4, 256: 2, 512: 1}},
}


class TrainingExamplesException(Exception):
    pass


class TrainingExamples(object):
    """

    """

    def __init__(self, conn, psyco_cur, quote_window, forecast_window, num_tickers, samples_per_ticker):
        """
        :param conn: psycopg2 connection object
        :param psyco_cur: psycopg2 connection cursor
        :type quote_window: int
        :param quote_window: length in days of input quote data to use per training example
        :type forecast_window: int
        :param forecast_window: length in days of quote data to forecast per training example
        :type num_tickers: int
        :param num_tickers: number of tickers to use in dataset
        :type samples_per_ticker: int
        :param samples_per_ticker: number of samples per ticker to use in dataset
        """
        if quote_window not in QUOTE_WINDOWS:
            msg = 'quote_window must be one of: %s' % str(sorted(QUOTE_WINDOWS.keys()))
            log.error(msg)
            raise TrainingExamplesException(msg)
        self.conn = conn
        self.psyco_cur = psyco_cur
        self.quote_window = quote_window
        self.forecast_window = forecast_window
        self.num_tickers = num_tickers
        self.samples_per_ticker = samples_per_ticker
        log.info('num_tickers             %d', self.num_tickers)
        log.info('samples_per_ticker      %d', self.samples_per_ticker)
        log.info('quote_window            %d', self.quote_window)
        log.info('forecast_window         %d', self.forecast_window)
        self.rand = SystemRandom()
        self.tickers = None
        self.sample_windows = list()
        self._retrieve_tickers()
        self._retrieve_quotes()

    def get_daily_price_count(self, symbol_id):
        """
        see how many quote days are in the db for this symbol_id
        :param symbol_id:
        :return:
        """
        self.psyco_cur.execute("select count(*) from t_daily_price where symbol_id = %d;" % symbol_id)
        daily_price_count = int(self.psyco_cur.fetchone()[0])
        log.info('symbol_id %d has %d daily price quotes', symbol_id, daily_price_count)
        assert isinstance(daily_price_count, int)
        return daily_price_count

    def _retrieve_tickers(self):
        """
        :return:
        """
        ticker_sql = 'select id, ticker from t_symbol'
        try:
            self.psyco_cur.execute(ticker_sql)
        except Exception as ex_obj:
            msg = 'query "%s" failed: %s' % (ticker_sql, str(ex_obj))
            log.error(msg)
            raise TrainingExamplesException(msg)

        rows = self.psyco_cur.fetchall()
        log.info('retrieved %d ticker rows', len(rows))
        self.tickers = list()
        for primary_key, ticker in rows:
            cnt = self.get_daily_price_count(primary_key)
            log.info('id %d, ticker %s, cnt %d', primary_key, ticker, cnt)
            if cnt > 0:
                self.tickers.append((primary_key, ticker))
            if self.num_tickers <= len(self.tickers):
                break
        if self.num_tickers > len(self.tickers):
            log.warning('num_tickers (%d) was reduced to %d, the number of tickers available',
                        self.num_tickers, len(self.tickers))
            self.num_tickers = len(self.tickers)
        self.rand.shuffle(self.tickers)

    def _retrieve_quotes(self):
        """

        :return:
        """
        for ticker_id, ticker in self.tickers:
            # Warning Never, never, NEVER use Python string concatenation (+) or string parameters
            # interpolation (%) to pass variables to a SQL query string. Not even at gunpoint.
            # http://initd.org/psycopg/docs/usage.html
            quote_sql = ('select price_date, open_price, high_price, low_price, close_price, adj_close_price, ' +
                         'percent_change from t_daily_price where symbol_id = %d LIMIT 10' % int(ticker_id))
            try:
                log.info('quote_sql: %s', quote_sql)
                self.psyco_cur.execute(quote_sql)
            except Exception as ex_obj:
                msg = 'query "%s" failed: %s' % (quote_sql, str(ex_obj))
                log.error(msg)
                raise TrainingExamplesException(msg)
            quotes = self.psyco_cur.fetchall()
            # sampling windows include a quote window, the forecast window, and the day before the beginning of
            # the quote window
            index_range = len(quotes) - self.quote_window - self.forecast_window - 1
            log.info('%d quotes, index_range %d', len(quotes), index_range)
            for _ in range(self.samples_per_ticker):
                index = self.rand.randint(0, index_range - 1)
                log.info('    index %d', index)
                # a[start:end] # items start through end-1
                # a[start:]    # items start through the rest of the array
                # a[:end]      # items from the beginning through end-1
                # a[:]         # a copy of the whole array
                # a[start:end:step] # start through not past end, by step
                sample_window = quotes[index:index + self.quote_window + self.forecast_window + 1]
                self.sample_windows.append(sample_window)
        self.rand.shuffle(self.sample_windows)

    def _build_training_examples(self):
        """

        :return:
        """
        for sample_window in self.sample_windows:
            # a stride is a contiguous length of quotes that will be aggregated into a close minus open set of metrics
            # for the quotes at the beginning and end of the stride
            for stride in sorted(QUOTE_WINDOWS[self.quote_window][KEY_STRIDES].keys()):
                stride_length = QUOTE_WINDOWS[self.quote_window][KEY_STRIDES][stride]
                for i in range(stride + 1):
                    log.info('stride %d, i %d, stride length %d', stride, stride_length, i)


if __name__ == '__main__':
    quote_data_log_format = ("%(asctime)s.%(msecs)03d %(levelname)-06s: " +
                             "%(module)s::%(funcName)s:%(lineno)s: %(message)s")
    quote_data_log_datefmt = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(level=logging.DEBUG,
                        format=quote_data_log_format,
                        datefmt=quote_data_log_datefmt)
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='cmd', help='Sub-commands')
    # create the parser for the "convert" command
    parser_convert = subparsers.add_parser('create', help='Train and test a datafile')
    parser_convert.add_argument("--output_data_file_path", "-o", required=True,
                                help="Path to output datafile", action="store")
    parser_convert.add_argument("--output_data_file_format", "-f", required=True,
                                help="Format of input datafile: {jsonz | keras_csv}", action="store")
    parser_convert.add_argument("--training_data", "-t", action="store_true",
                                help="If present, output data will be written to file. " +
                                     "Otherwise it is a test data file with no outputs.")
    parser_convert.add_argument("--num_tickers", "-n", required=True,
                                help="Number of tickers to use", action="store")
    parser_convert.add_argument("--samples_per_ticker", "-s", required=True,
                                help="Number of intervals to use per ticker", action="store")
    parser_convert.add_argument("--quote_window", "-w", required=True,
                                help="Size of input quote window in days", action="store")
    parser_convert.add_argument("--forecast_window", "-p", required=True,
                                help="Size of forecast quote window in days", action="store")

    args = parser.parse_args()

    database = 'trade_forecast'
    user = 'trade_forecast'
    password = '867-5309'

    try:
        test_conn = psycopg2.connect("dbname='%s' user='%s' host='localhost' password='%s'" %
                                     (database, user, password))
    except:
        raise Exception("Unable to connect to database %s" % database)
    log.info("connected to database %s", database)
    test_psyco_cur = test_conn.cursor()

    log.info('output_data_file_path   %s', str(args.output_data_file_path))
    log.info('output_data_file_format %s', str(args.output_data_file_format))
    log.info('training_data           %s', str(args.training_data))
    log.info('num_tickers             %s', str(args.num_tickers))
    log.info('samples_per_ticker      %s', str(args.samples_per_ticker))
    log.info('quote_window            %s', str(args.quote_window))
    log.info('forecast_window         %s', str(args.forecast_window))

    try:
        if args.cmd == 'create':
            log.info('output will be written to %s', args.output_data_file_path)
            test_examples = TrainingExamples(test_conn, test_psyco_cur,
                                             int(args.quote_window), int(args.forecast_window),
                                             int(args.num_tickers), int(args.samples_per_ticker))

        sys.exit(0)
    except TrainingExamplesException as examples_exception:
        logging.exception(examples_exception)
    except Exception as generic_exception:
        logging.exception(generic_exception)
    finally:
        logging.shutdown()
        sys.exit(1)
