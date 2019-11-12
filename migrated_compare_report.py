#!/usr/bin/env python

# Copyright 2017-present, Bill & Melinda Gates Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import csv
import logging
import argparse
import datetime
import time
from utils import Utils
from ghap_migrator import GhapMigrator


class MigratedCompareReport:

    def __init__(self, csv_file_path, checkout_root_dir):
        self._csv_file_path = csv_file_path
        self._checkout_root_dir = Utils.expand_path(checkout_root_dir)
        self._csv_data = []

        self._errors = []
        self._start_time = None
        self._end_time = None

    def log_error(self, msg):
        if msg not in self._errors:
            self._errors.append(msg)
        logging.error(msg)

    def start(self):
        self._start_time = time.time()
        self._csv_data = []

        logging.info("Started at: {0}".format(datetime.datetime.now()))
        logging.info('Git Checkout Roo Directory: {0}'.format(self._checkout_root_dir))
        logging.info('CSV File: {0}'.format(self._csv_file_path))

        self._load_csv()
        self._compare(self._checkout_root_dir)

        logging.info('#' * 80)

        run_duration = datetime.timedelta(seconds=(time.time() - self._start_time))
        logging.info("Ended at: {0}, total duration: {1}".format(datetime.datetime.now(), run_duration))

        if len(self._errors) > 0:
            logging.info('!' * 80)
            logging.info('Completed with Errors:')
            for line in self._errors:
                logging.error(' - {0}'.format(line))
        else:
            logging.info('Completed Successfully.')

    def _compare(self, local_path):
        dirs, files = Utils.get_dirs_and_files(local_path)

        for file in files:
            row = self._find_csv_row(file.path)
            if row:
                logging.info('[FILE MIGRATED] {0}'.format(file.path))
            else:
                if os.path.getsize(file.path) > 0:
                    self.log_error('[FILE NOT MIGRATED] {0}'.format(file.path))
                else:
                    logging.warning('[FILE NOT MIGRATED] [HAS ZERO SIZE] {0}'.format(file.path))

        for dir in dirs:
            # row = self._find_csv_row(dir.path)
            # if row:
            #     logging.info('[DIRECTORY MIGRATED] {0}'.format(dir.path))
            # else:
            #     self.log_error('[DIRECTORY NOT MIGRATED] {0}'.format(dir.path))
            self._compare(dir.path)

    def _load_csv(self):
        with open(self._csv_file_path) as csv_file:
            reader = csv.DictReader(csv_file, delimiter=',', fieldnames=GhapMigrator.WRITE_CSV_LINES_HEADERS)
            # Skip the header row
            next(reader)

            for row in reader:
                self._csv_data.append(row)
        logging.info('CSV Rows Loaded: {0}'.format(len(self._csv_data)))

    def _find_csv_row(self, local_path):
        results = [r for r in self._csv_data if r['local_path'] == local_path]
        if len(results) > 1:
            self.log_error('Found more than one file matching path: {0}'.format(local_path))
            return results[0]
        elif len(results) == 1:
            return results[0]
        else:
            return None


def main():
    try:
        parser = argparse.ArgumentParser(
            description='Compares a local directory of files against the ghap_migrator\'s processed CSV file to ensure all files in a local directory have been migrated.')
        parser.add_argument('csv', help='The processed CSV file path.', default=None)
        parser.add_argument('checkout_root', metavar='checkout-root',
                            help='The directory containing the root of the repo checkouts.',
                            default=None)
        parser.add_argument('-l', '--log-level', help='Set the logging level.', default='INFO')

        args = parser.parse_args()

        log_level = getattr(logging, args.log_level.upper())
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        log_filename = 'migrated_compare_report_log_{0}.txt'.format(timestamp)
        Utils.setup_logging(log_filename, log_level)

        MigratedCompareReport(
            args.csv,
            args.checkout_root
        ).start()
    except Exception as ex:
        logging.exception('Unhandled exception: {0}'.format(ex))


if __name__ == "__main__":
    main()
