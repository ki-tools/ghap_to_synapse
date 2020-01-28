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
from utils import Utils


class FilenameFixer:

    def __init__(self, starting_path, dry_run=False, replace_char=''):
        self._starting_path = Utils.expand_path(starting_path)
        self._dry_run = dry_run
        self._replace_char = replace_char
        self.renamed = []
        self.errors = []

    def execute(self):
        fixed_dir_path = self.check_for_rename(self._starting_path)
        self.process_dir(fixed_dir_path)

        logging.info('')
        logging.info('=' * 80)
        logging.info('')

        if self.renamed:
            logging.info('')
            logging.info('Renamed Directories and Files:')
            for name in self.renamed:
                logging.info('  {0}'.format(name))
            logging.info('')

        if self.errors:
            logging.info('')
            logging.info('Errors:')
            for error in self.errors:
                logging.info('  {0}'.format(error))
            logging.info('')
            logging.info('Finished with errors.')
        else:
            logging.info('')
            logging.info('Finished successfully.')

        if self._dry_run:
            logging.info('')
            logging.info('!' * 80)
            logging.info('!!! Dry Run Only - No Files or Directories Changed !!!')
            logging.info('!' * 80)
            logging.info('')

    def process_dir(self, local_path):
        logging.info('-' * 80)
        logging.info('Processing: {0}'.format(local_path))
        dirs, files = Utils.get_dirs_and_files(local_path)
        for file in files:
            self.check_for_rename(file.path)

        for dir in dirs:
            fixed_dir_path = self.check_for_rename(dir.path)
            self.process_dir(fixed_dir_path)

    def check_for_rename(self, local_path):
        try:
            name = os.path.basename(local_path)
            invalid_chars = list(set(Utils.get_invalid_synapse_name_chars(name)))
            if invalid_chars:
                new_name = name
                for invalid_char in invalid_chars:
                    new_name = new_name.replace(invalid_char, self._replace_char)

                logging.info('  - Renaming: {0} -> {1} - Removing: {2}'.format(name, new_name, ''.join(invalid_chars)))
                new_full_path = os.path.join(os.path.dirname(local_path), new_name)

                if os.path.exists(new_full_path):
                    self.errors.append('Name collision. Cannot rename: {0} -> {1}'.format(local_path, new_full_path))
                    logging.info(self.errors[-1])
                    return local_path

                if self._dry_run:
                    self.renamed.append('{0} -> {1}'.format(local_path, new_full_path))
                    return local_path
                else:
                    os.rename(local_path, new_full_path)
                    self.renamed.append('{0} -> {1}'.format(local_path, new_full_path))
                    return new_full_path
        except Exception as ex:
            self.errors.append('Error renaming: {0} : {1}'.format(local_path, ex))
            logging.info(self.errors[-1])

        return local_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='Path to the directory to fix all folder and file names.')
    parser.add_argument('-d', '--dry-run', help='Do a dry run and don\'t rename anything',
                        default=False,
                        action='store_true')
    parser.add_argument('-c', '--replace-char', help='The character to use to replace the invalid characters',
                        default='')
    parser.add_argument('-l', '--log-level', help='Set the logging level.', default='INFO')

    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper())
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    log_filename = 'filename_fixer_log_{0}.txt'.format(timestamp)
    Utils.setup_logging(log_filename, log_level)

    FilenameFixer(args.path, dry_run=args.dry_run, replace_char=args.replace_char).execute()


if __name__ == "__main__":
    main()
