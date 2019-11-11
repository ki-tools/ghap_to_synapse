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
import logging
import argparse
import datetime
import time
import synapseclient
from utils import Utils
from aio_manager import AioManager
from synapse_proxy import SynapseProxy
from file_handle_view import FileHandleView


class InSynapseReport:

    def __init__(self, work_dir, start_dir, username=None, password=None):
        self._work_dir = Utils.expand_path(work_dir)
        self._start_dir = Utils.expand_path(start_dir)
        self._username = username
        self._password = password

        self._errors = []
        self._start_time = None
        self._end_time = None

    def log_error(self, msg):
        if msg not in self._errors:
            self._errors.append(msg)
        logging.error(msg)

    def start(self):
        self._start_time = time.time()

        logging.info("Started at: {0}".format(datetime.datetime.now()))
        logging.info('Working Directory: {0}'.format(self._work_dir))
        logging.info('Starting Directory: {0}'.format(self._start_dir))

        if not SynapseProxy.login(self._username, self._password):
            self.log_error('Synapse login failed: {0}'.format(SynapseProxy.login_error))
        else:
            AioManager.start(self._startAsync)

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

    async def _startAsync(self):

        for dirpath, _, filenames in os.walk(self._start_dir):
            if '.git' in dirpath.split(os.sep):
                logging.info('Skipping: {0}'.format(dirpath))
                continue

            for filename in filenames:
                full_file_path = os.path.join(dirpath, filename)

                found = False
                try:
                    # results = await SynapseProxy.getFromFileAsync(full_file_path)
                    results = await SynapseProxy.Aio.get_from_file(full_file_path)
                    if results:
                        found = True
                        logging.info('Found {0} results for: {1}'.format(len(results), full_file_path))
                        for result in results:
                            logging.info(
                                '  - Found at: {0}({1})'.format(result['name'], result['id']))
                except synapseclient.exceptions.SynapseFileNotFoundError as ex:
                    # Not found
                    pass

                if not found:
                    self.log_error('NOT Found in Synapse: {0}'.format(full_file_path))


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('work', help='The work directory', default=None)
        parser.add_argument('start', help='The directory containing all the files to check.', default=None)
        parser.add_argument('-u', '--username', help='Synapse username.', default=None)
        parser.add_argument('-p', '--password', help='Synapse password.', default=None)
        parser.add_argument('-l', '--log-level', help='Set the logging level.', default='INFO')

        args = parser.parse_args()

        log_level = getattr(logging, args.log_level.upper())
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        log_filename = 'in_synapse_report_log_{0}.txt'.format(timestamp)
        Utils.setup_logging(log_filename, log_level)

        InSynapseReport(
            args.work,
            args.start,
            username=args.username,
            password=args.password
        ).start()
    except Exception as ex:
        logging.exception('Unhandled exception: {0}'.format(ex))


if __name__ == "__main__":
    main()
