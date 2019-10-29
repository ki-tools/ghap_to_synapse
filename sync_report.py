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


class SynReport:
    def __init__(self, csv_filename, username=None, password=None, work_dir=None):
        self._csv_filename = csv_filename
        self._username = username
        self._password = password
        self._work_dir = None

        if work_dir is None:
            self._work_dir = Utils.expand_path(os.path.join('~', 'tmp', 'ghap'))
        else:
            self._work_dir = Utils.expand_path(work_dir)

        self._errors = []
        self._start_time = None
        self._end_time = None

    def log_error(self, msg):
        if msg not in self._errors:
            self._errors.append(msg)
        logging.error(msg)

    def start(self):
        self._start_time = time.time()
        if not os.path.exists(self._work_dir):
            os.makedirs(self._work_dir)

        logging.info("Started at: {0}".format(datetime.datetime.now()))
        logging.info('CSV File: {0}'.format(self._csv_filename))
        logging.info('Work Directory: {0}'.format(self._work_dir))

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
        await Utils.process_repo_csv(self._csv_filename, self._work_dir, self.report_on_repo, self.log_error)

    async def report_on_repo(self, git_url, repo_name, repo_path, git_folder, synapse_project_id, synapse_path):
        logging.info('-' * 80)
        project_id_or_name = synapse_project_id if synapse_project_id else Utils.create_project_name(repo_name,
                                                                                                     git_folder)

        logging.info('GIT Folder: {0}'.format(git_folder))
        logging.info('Project ID: {0}'.format(synapse_project_id))
        logging.info('Project Path: {0}'.format(synapse_path))
        logging.info('')

        project = await self.find_project(project_id_or_name)
        if project:
            logging.info('[Project FOUND] {0}: {1}'.format(project.id, project.name))
        else:
            logging.info('[Project NOT Found] {0}'.format(synapse_project_id))
            return

        if synapse_path:
            path_parent = project
            syn_path_parts = Utils.get_path_parts(synapse_path)

            for folder_name in syn_path_parts:
                syn_folder = await self.find_child(path_parent, folder_name, synapseclient.Folder)

                full_syn_path = os.path.join(project.id, synapse_path)
                if syn_folder:
                    path_parent = syn_folder
                    logging.info(
                        'SUCCESS: Synapse Folder found for: "{0}" in "{1}"'.format(folder_name, full_syn_path))
                else:
                    logging.info(
                        'FAIL: Synapse Folder not found for: "{0}" in "{1}"'.format(folder_name, full_syn_path))
                    return

        start_path = repo_path
        if git_folder:
            start_path = os.path.join(repo_path, git_folder)

        dirs, files = Utils.get_dirs_and_files(start_path)

    def check_path(self, local_path, remote_parent):
        pass

    # async def get_syn_folder_files(self, syn_parent):
    #     children = await list(SynapseProxy.getChildrenAsync(syn_parent, includeTypes=["folder", "file"]))
    #     return children

    async def find_child(self, syn_parent, child_name, syn_type):
        """Tries to find the child of a parent of a particular name and type (folder/file)."""
        syn_id = await SynapseProxy.findEntityIdAsync(child_name, parent=syn_parent)
        if syn_id:
            syn_obj = await SynapseProxy.getAsync(syn_id)
            if isinstance(syn_obj, syn_type):
                return syn_obj
        return None

    async def find_project(self, project_name_or_id):
        project = None

        try:
            if project_name_or_id.lower().startswith('syn'):
                project = SynapseProxy.getAsync(project_name_or_id)
            else:
                project_id = await SynapseProxy.findEntityIdAsync(project_name_or_id)
                project = await SynapseProxy.getAsync(project_id)
        except synapseclient.exceptions.SynapseHTTPError as ex:
            if ex.response.status_code >= 400:
                self.log_error('Script user does not have READ permission to Project: {0}'.format(project_name_or_id))
                return None
        except Exception as ex:
            # Project doesn't exist.
            pass

        return project


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            'csv', help='CSV file with GIT repository URLs to process.')
        parser.add_argument('-u', '--username', help='Synapse username.', default=None)
        parser.add_argument('-p', '--password', help='Synapse password.', default=None)
        parser.add_argument('-w', '--work-dir', help='The directory to git pull repos into.', default=None)
        parser.add_argument('-l', '--log-level', help='Set the logging level.', default='INFO')

        args = parser.parse_args()

        log_level = getattr(logging, args.log_level.upper())
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        log_filename = 'sync_report_log_{0}.txt'.format(timestamp)
        Utils.setup_logging(log_filename, log_level)

        SynReport(
            args.csv,
            username=args.username,
            password=args.password,
            work_dir=args.work_dir
        ).start()
    except Exception as ex:
        logging.exception('Unhandled exception: {0}'.format(ex))


if __name__ == "__main__":
    main()
