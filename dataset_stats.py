#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Describe what this module does

Unit tests and useful commands:

./module_template.py my_function arf
./module_template.py my_function --double arf
./module_template.py calc_stats arf
./module_template.py write_data_set arf
./module_template.py calc_stats --data_file_path xxx arf
./module_template.py write_data_set --write_ticker_files  arf

"""
import json
import codecs
import time
import re
import datetime
import random
import logging
import sys
import os
import argparse
import numpy as np

# from Tintest.System.Tintri.Realstore import Realstore

__author__ = 'hhemken'
log = logging.getLogger(__file__)


class DatasetStatsException(Exception):
    """
    Generic exception to be dataset_stats within the DatasetStats class.
    """

    def __init__(self, msg):
        log.exception(msg)
        super(DatasetStatsException, self).__init__(msg)


class DatasetStats(object):
    """
    Describe what DatasetStats is for
    """

    PCT_CHG_CEILING = 50.0
    PCT_CHG_FLOOR = -25.0
    PCT_CHG_RANGE = PCT_CHG_CEILING - PCT_CHG_FLOOR
    ANOTHER_CLASS_CONSTANT = 30

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.raw_data = list()

    def scale_percent_change(self, percent_change):
        """

        :param datum:
        :return:
        """
        if percent_change < self.PCT_CHG_FLOOR:
            percent_change = self.PCT_CHG_FLOOR
        elif percent_change > self.PCT_CHG_CEILING:
            percent_change = self.PCT_CHG_CEILING
        return (percent_change - self.PCT_CHG_FLOOR) / self.PCT_CHG_RANGE

    def load_data(self, data_file_path):
        """
        load the data file into an instance variable
        :param data_file_path Describe my_arg
        :return None
        """
        log.info('loading file "%s"', data_file_path)
        with open(data_file_path, 'r') as f:
            for line in f:
                try:
                    # self.raw_data += [self.scale_percent_change(float(x)) for x in line.strip().split()]
                    self.raw_data += [float(x) for x in line.strip().split()]
                except ValueError as ex_obj:
                    log.error('line %s: %s', line, str(ex_obj))
        np_counts = np.array(self.raw_data)
        log.info('----------' * 8)
        log.info('%d numbers', len(self.raw_data))
        log.info('    avg: %8.3f +/- %8.3f', np.mean(np_counts), np.std(np_counts))
        log.info('    max: %8.3f', np.max(np_counts))
        log.info('    min: %8.3f', np.min(np_counts))
        log.info('     1:  %8.3f', np.percentile(np_counts, 1))
        log.info('     2:  %8.3f', np.percentile(np_counts, 2))
        log.info('     5:  %8.3f', np.percentile(np_counts, 5))
        log.info('    10:  %8.3f', np.percentile(np_counts, 10))
        log.info('    20:  %8.3f', np.percentile(np_counts, 20))
        log.info('    30:  %8.3f', np.percentile(np_counts, 30))
        log.info('    40:  %8.3f', np.percentile(np_counts, 40))
        log.info('    50:  %8.3f', np.percentile(np_counts, 50))
        log.info('    60:  %8.3f', np.percentile(np_counts, 60))
        log.info('    70:  %8.3f', np.percentile(np_counts, 70))
        log.info('    80:  %8.3f', np.percentile(np_counts, 80))
        log.info('    90:  %8.3f', np.percentile(np_counts, 90))
        log.info('    95:  %8.3f', np.percentile(np_counts, 95))
        log.info('    98:  %8.3f', np.percentile(np_counts, 98))
        log.info('    99:  %8.3f', np.percentile(np_counts, 99))


if __name__ == '__main__':
    tintri_log_format = ("%(asctime)s.%(msecs)03d [%(process)d] %(threadName)s: %(levelname)-06s: " +
                         "%(module)s::%(funcName)s:%(lineno)s: %(message)s")
    tintri_log_datefmt = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(level=logging.DEBUG,
                        format=tintri_log_format,
                        datefmt=tintri_log_datefmt)

    parent_parser = argparse.ArgumentParser(add_help=False)
    # parser.add_argument('required_option', help='An option that always needs to be specified on the command line')
    parent_parser.add_argument('--data_file_path', default=False,
                               help="Path of data file.")
    parser = argparse.ArgumentParser(add_help=False)
    subparsers = parser.add_subparsers(dest='command')
    # calc_stats command line command
    calc_stats_parser = subparsers.add_parser('calc_stats', parents=[parent_parser], help="Will run calc_stats.")

    args = parser.parse_args()
    log.info('args: %s', str(args))
    test_dataset_stats_util = DatasetStats()

    exit_code = 1
    try:
        if args.command == 'calc_stats':
            if args.data_file_path:
                log.info("data file path   %s", args.data_file_path)
                test_dataset_stats_util.load_data(args.data_file_path)
            else:
                raise DatasetStatsException("missing data file path")

        exit_code = 0
    except DatasetStatsException as dataset_stats_exception:
        log.exception(dataset_stats_exception)
    except Exception as generic_exception:
        logging.exception(generic_exception)
    finally:
        logging.shutdown()
    sys.exit(exit_code)
