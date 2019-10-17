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
import argparse
import logging
import getpass
import sh
import time
import datetime
import random
import csv
import concurrent.futures
import threading
import synapseclient
from synapseclient import Project, Folder, File
from io import StringIO
import urllib.parse as UrlParse
from utils import Utils


class GhapMigrator:

    def __init__(self, csv_filename, username=None, password=None, admin_team_id=None, storage_location_id=None,
                 max_threads=None, work_dir=None):
        self._csv_filename = csv_filename
        self._username = username
        self._password = password
        self._admin_team_id = admin_team_id
        self._admin_team = None
        self._storage_location_id = storage_location_id
        self._storage_location = None
        self._work_dir = None

        if work_dir is None:
            self._work_dir = Utils.expand_path(os.path.join('~', 'tmp', 'ghap'))
        else:
            self._work_dir = Utils.expand_path(work_dir)

        self._synapse_client = None
        self._script_user = None
        self._synapse_parents = {}
        self._git_to_syn_mappings = []
        self._stats = {
            'found': [],
            'processed': []
        }
        self._full_synapse_paths = []
        self._errors = []
        self._thread_lock = threading.Lock()
        self._max_threads = max_threads
        self._start_time = None
        self._end_time = None
        self._git_lfs_installed = False

    def log_error(self, msg):
        """
        Logs the error message and adds it to the errors list.
        """
        with self._thread_lock:
            # Do not add duplicate errors.
            if msg not in self._errors:
                self._errors.append(msg)
            logging.error(msg)

    def add_processed_path(self, path):
        """
        Adds a file/folder path to the stats.
        This is used to verify each local file is uploaded to Synapse.
        """
        with self._thread_lock:
            self._stats['processed'].append(path)

    def add_full_synapse_path(self, full_synapse_path, local_path):
        """
        Keeps track of every file and folder created or uploaded to Synapse.
        This is used to make sure sanitized filenames don't collide with other files.
        """
        with self._thread_lock:
            if full_synapse_path in self._full_synapse_paths:
                raise Exception('Duplicate synapse path found: {0} for: {1}'.format(full_synapse_path, local_path))
            else:
                self._full_synapse_paths.append(full_synapse_path)

    def check_git_lfs(self):
        """
        # Warn if git lfs is not installed.
        """
        try:
            sh.git('lfs')
            return True
        except sh.ErrorReturnCode as ex:
            logging.warning('!' * 80)
            logging.warning('GIT LFS not installed.')
            logging.warning('!' * 80)
        return False

    def start(self):
        self._start_time = time.time()
        if not os.path.exists(self._work_dir):
            os.makedirs(self._work_dir)

        self._git_lfs_installed = self.check_git_lfs()

        logging.info("Started at: {0}".format(datetime.datetime.now()))
        logging.info('CSV File: {0}'.format(self._csv_filename))
        logging.info('Work Directory: {0}'.format(self._work_dir))

        self.synapse_login()
        self._script_user = self._synapse_client.getUserProfile()

        if self._admin_team_id and self._admin_team_id.strip() != '':
            logging.info('Loading Admin Team ID: {0}'.format(self._admin_team_id))
            self._admin_team = self._synapse_client.getTeam(self._admin_team_id)
            logging.info('Admin Team Loaded: {0}'.format(self._admin_team.name))
        else:
            self._admin_team_id = None

        if self._storage_location_id and self._storage_location_id.strip() != '':
            logging.info('Loading Storage Location ID: {0}'.format(self._storage_location_id))
            self._storage_location = self._synapse_client.getMyStorageLocationSetting(self._storage_location_id)
            logging.info('Storage Location: {0}'.format(self._storage_location['bucket']))
        else:
            self._storage_location_id = None

        self.process_csv()

        logging.info('#' * 80)

        run_duration = datetime.timedelta(seconds=(time.time() - self._start_time))
        logging.info("Ended at: {0}, total duration: {1}".format(datetime.datetime.now(), run_duration))

        for path in self._stats['found']:
            if path not in self._stats['processed']:
                self._errors.append('Path found but not processed: {0}'.format(path))

        if len(self._git_to_syn_mappings) > 0:
            logging.info('Synapse Projects:')
            for line in self._git_to_syn_mappings:
                logging.info(' - {0}'.format(line))

        if len(self._errors) > 0:
            logging.info('!' * 80)
            logging.info('Completed with Errors:')
            for line in self._errors:
                logging.error(' - {0}'.format(line))
        else:
            logging.info('Completed Successfully.')

    def synapse_login(self):
        logging.info('Logging into Synapse...')
        self._username = self._username or os.getenv('SYNAPSE_USERNAME')
        self._password = self._password or os.getenv('SYNAPSE_PASSWORD')

        if not self._username:
            self._username = input('Synapse username: ')

        if not self._password:
            self._password = getpass.getpass(prompt='Synapse password: ')

        try:
            self._synapse_client = synapseclient.Synapse()
            self._synapse_client.login(self._username, self._password, silent=True)
        except Exception as ex:
            self._synapse_client = None
            self.log_error('Synapse login failed: {0}'.format(str(ex)))

    def process_csv(self):
        """
        Process the CSV file.
        The format CSV format is: "git_url,synapse_project_id"
          git_url:            The full GIT URL of the repository to migrate.
          git_folder:         The folder within the repo to migrate.
                              Blank = Migrate the whole repo.
                              Name = Only migrate the specific folder.
          synapse_project_id: The Synapse Project to migrate the repository into.
                              Blank = Create a new Project.
                              SynID = Use an existing Project and upload into a new Folder in the Project.
          synapse_path:       The folder path in Synapse to store the files (e.g., EDD/common)
        """
        with open(self._csv_filename) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            for row in reader:
                git_url = row['git_url'].strip()
                git_folder = row['git_folder'].strip()
                synapse_project_id = row['synapse_project_id'].strip()
                synapse_path = row['synapse_path'].lstrip(os.sep).rstrip(os.sep)
                self.migrate(git_url, git_folder, synapse_project_id, synapse_path)

    def migrate(self, git_url, git_folder, synapse_project_id, synapse_path):
        logging.info('=' * 80)
        logging.info('Processing {0}'.format(git_url))
        if git_folder:
            logging.info('  - Folder: {0}'.format(git_folder))

        repo_url_path = UrlParse.urlparse(git_url).path.replace('.git', '').lstrip('/')
        repo_name = repo_url_path.split('/')[-1]
        repo_path = os.path.join(self._work_dir, repo_url_path)

        git_exception = None

        # Use 'lfs' for git commands to get around memory constraints when using
        # the normal 'git clone'/'git pull' commands.
        # https://github.com/git-lfs/git-lfs/issues/3524
        lfs = 'lfs' if self._git_lfs_installed else ''

        if os.path.exists(repo_path):
            # Pull
            logging.info('  - Pulling Repo into {0}'.format(repo_path))
            try:
                if lfs:
                    sh.git.bake(_cwd=repo_path).lfs('pull')
                else:
                    sh.git.bake(_cwd=repo_path).pull()
            except Exception as ex:
                git_exception = ex
        else:
            # Checkout
            logging.info('  - Cloning into {0}'.format(repo_path))
            try:
                if lfs:
                    sh.git.bake(_cwd=self._work_dir).lfs('clone', git_url, repo_path)
                else:
                    sh.git.bake(_cwd=self._work_dir).clone(git_url, repo_path)
            except Exception as ex:
                git_exception = ex

        if git_exception:
            self.log_error('Error pulling repo: {0} : {1}'.format(git_url, str(git_exception)))
        else:
            self.push_to_synapse(git_url, repo_name, repo_path, git_folder, synapse_project_id, synapse_path)

    def push_to_synapse(self, git_url, repo_name, repo_path, git_folder, synapse_project_id, synapse_path):
        project = None

        if synapse_project_id and synapse_project_id != '':
            # Find or create a Folder in the Project to store the repo.
            project = self.find_or_create_project(synapse_project_id)
        else:
            # Find or Create the Project.
            project_name = 'GHAP - {0}'.format(repo_name)
            if git_folder:
                project_name += ' - {0}'.format(git_folder.replace('/', '-'))
            project = self.find_or_create_project(project_name)

        if not project:
            self.log_error('Could not get project for {0}.'.format(git_url))
            return
        else:
            if git_folder:
                self._git_to_syn_mappings.append(
                    '{0} ({1}) -> {2} ({3})'.format(git_url, git_folder, project.name, project.id))
            else:
                self._git_to_syn_mappings.append('{0} -> {1} ({2})'.format(git_url, project.name, project.id))

        parent = project

        # Create the folders if specified.
        if synapse_path:
            full_path = ''
            for folder in filter(None, synapse_path.split(os.sep)):
                full_path = os.path.join(full_path, folder)
                parent = self.find_or_create_folder(full_path, parent)

        start_path = repo_path
        if git_folder:
            start_path = os.path.join(repo_path, git_folder)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_threads) as executor:
            self.upload_folder(executor, start_path, parent)

    def upload_folder(self, executor, local_path, synapse_parent):
        try:
            if not synapse_parent:
                self.log_error('Parent not found, cannot upload folder: {0}'.format(local_path))
                return

            parent = synapse_parent

            dirs, files = Utils.get_dirs_and_files(local_path)
            with self._thread_lock:
                self._stats['found'] += [dir.path for dir in dirs] + [file.path for file in files]

            # Upload the files
            for file_entry in files:
                # Create the GIT log for the file.
                # filename = os.path.basename(file_entry.path)
                # dirpath = os.path.dirname(file_entry.path)

                # git_log_filename = os.path.join(dirpath, '{0}.gitlog'.format(filename))
                # sh.git.bake('--no-pager', _cwd=dirpath).log(
                #     '--pretty=commit %H%nDate: %cd%nAuthor: %an%nSubject: %s%nNotes:%N%n', filename, _out=git_log_filename,
                #     _tty_out=False)

                executor.submit(self.find_or_upload_file, file_entry.path, parent)
                # executor.submit(self.find_or_upload_file, git_log_filename, parent)

            # Upload the directories.
            for dir_entry in dirs:
                syn_dir = self.find_or_create_folder(dir_entry.path, parent)
                self.upload_folder(executor, dir_entry.path, syn_dir)
        except Exception as ex:
            self.log_error('Error uploading folder: {0}, {1}'.format(local_path, ex))

    def find_or_create_project(self, project_name_or_id):
        project = None

        try:
            if project_name_or_id.lower().startswith('syn'):
                project = self._synapse_client.get(project_name_or_id)
            else:
                project_id = self._synapse_client.findEntityId(project_name_or_id)
                project = self._synapse_client.get(project_id)
        except synapseclient.exceptions.SynapseHTTPError as ex:
            if ex.response.status_code >= 400:
                self.log_error('Script user does not have READ permission to Project: {0}'.format(project_name_or_id))
                return None
        except Exception as ex:
            # Project doesn't exist.
            pass

        if project:
            logging.info('[Project FOUND] {0}: {1}'.format(project.id, project.name))
            if not self.has_write_permissions(project):
                self.log_error('Script user does not have WRITE permission to Project: {0}'.format(project_name_or_id))
                return None
        else:
            try:
                project = self._synapse_client.store(Project(project_name_or_id))
                logging.info('[Project CREATED] {0}: {1}'.format(project.id, project.name))
                if self._storage_location_id:
                    logging.info('Setting storage location for project: {0}: {1}'.format(project.id, project.name))
                    self._synapse_client.setStorageLocation(project, self._storage_location_id)

                if self._admin_team:
                    logging.info(
                        'Granting admin permissions to team on Project: {0}: {1}'.format(project.id, project.name))
                    self.grant_admin_access(project, self._admin_team.id)
            except Exception as ex:
                self.log_error('Error creating project: {0}, {1}'.format(project_name_or_id, ex))

        if project:
            self.set_synapse_parent(project)

        return project

    def has_write_permissions(self, project):
        # Check for user specific permissions.
        user_perms = set(self._synapse_client.getPermissions(project, self._script_user.ownerId))
        if ('CREATE' in user_perms) and ('UPDATE' in user_perms):
            return True

        # Check for team permissions.
        acl = self._synapse_client._getACL(project)

        for resourceAccess in acl['resourceAccess']:
            principalId = resourceAccess['principalId']
            try:
                team = self._synapse_client.getTeam(principalId)
                team_members = self._synapse_client.getTeamMembers(team)
                for team_member in team_members:
                    if team_member['member']['ownerId'] == self._script_user.ownerId:
                        team_perms = set(resourceAccess['accessType'])
                        if ('CREATE' in team_perms) and ('UPDATE' in team_perms):
                            return True
            except synapseclient.exceptions.SynapseHTTPError as ex:
                # This will 404 when fetching a User instead of a Team.
                if ex.response.status_code != 404:
                    raise ex

    def grant_admin_access(self, project, grantee_id):
        accessType = ['UPDATE', 'DELETE', 'CHANGE_PERMISSIONS',
                      'CHANGE_SETTINGS', 'CREATE', 'DOWNLOAD', 'READ', 'MODERATE']
        self._synapse_client.setPermissions(project, grantee_id, accessType=accessType, warn_if_inherits=False)

    def find_or_create_folder(self, path, synapse_parent):
        synapse_folder = None

        if not synapse_parent:
            self.log_error('Parent not found, cannot create folder: {0}'.format(path))
            return synapse_folder

        folder_name = os.path.basename(path)

        bad_name_chars = Utils.get_invalid_synapse_name_chars(folder_name)
        if bad_name_chars:
            self.log_error(
                'Folder name: "{0}" contains invalid characters: "{1}"'.format(path, ''.join(bad_name_chars)))
            return synapse_folder

        full_synapse_path = self.get_synapse_path(folder_name, synapse_parent)

        syn_folder_id = self._synapse_client.findEntityId(folder_name, parent=synapse_parent)

        if syn_folder_id:
            synapse_folder = self._synapse_client.get(syn_folder_id, downloadFile=False)
            self.set_synapse_parent(synapse_folder)
            self.add_processed_path(path)
            logging.info('[Folder EXISTS]: {0} -> {1}'.format(path, full_synapse_path))
        else:
            max_attempts = 5
            attempt_number = 0
            exception = None

            while attempt_number < max_attempts and not synapse_folder:
                try:
                    attempt_number += 1
                    exception = None
                    synapse_folder = self._synapse_client.store(
                        Folder(name=folder_name, parent=synapse_parent), forceVersion=False)
                except Exception as ex:
                    exception = ex
                    self.log_error('[Folder ERROR] {0} -> {1} : {2}'.format(path, full_synapse_path, str(ex)))
                    if attempt_number < max_attempts:
                        sleep_time = random.randint(1, 5)
                        logging.info('[Folder RETRY in {0}s] {1} -> {2}'.format(sleep_time, path, full_synapse_path))
                        time.sleep(sleep_time)

            if exception:
                self.log_error('[Folder FAILED] {0} -> {1} : {2}'.format(path, full_synapse_path, str(exception)))
            else:
                self.add_processed_path(path)
                logging.info('[Folder CREATED] {0} -> {1}'.format(path, full_synapse_path))
                self.set_synapse_parent(synapse_folder)

        return synapse_folder

    def find_or_upload_file(self, local_file, synapse_parent):
        synapse_file = None
        try:
            if not synapse_parent:
                self.log_error('Parent not found, cannot upload file: {0}'.format(local_file))
                return synapse_file

            if os.path.getsize(local_file) < 1:
                logging.info('Skipping Empty File: {0}'.format(local_file))
                self.add_processed_path(local_file)
                return synapse_file

            filename = os.path.basename(local_file)

            bad_name_chars = Utils.get_invalid_synapse_name_chars(filename)
            if bad_name_chars:
                self.log_error(
                    'File name: "{0}" contains invalid characters: "{1}"'.format(local_file, ''.join(bad_name_chars)))
                return synapse_file

            full_synapse_path = self.get_synapse_path(filename, synapse_parent)
            self.add_full_synapse_path(full_synapse_path, local_file)

            # Check if the file has already been uploaded and has not changed since being uploaded.
            syn_file_id = self._synapse_client.findEntityId(filename, parent=synapse_parent)
            local_md5 = self.get_local_file_md5(local_file)

            if syn_file_id:

                synapse_file = self._synapse_client.get(syn_file_id, downloadFile=False)
                synapse_file_md5 = synapse_file._file_handle['contentMd5']

                if local_md5 == synapse_file_md5:
                    logging.info('[File is CURRENT] {0} -> {1}'.format(local_file, full_synapse_path))
                    self.add_processed_path(local_file)
                    return synapse_file
                else:
                    logging.info('[File has CHANGES] {0} -> {1}'.format(local_file, full_synapse_path))
                    synapse_file = None

            max_attempts = 5
            attempt_number = 0
            exception = None

            while attempt_number < max_attempts and not synapse_file:
                try:
                    attempt_number += 1
                    exception = None
                    synapse_file = self._synapse_client.store(
                        File(path=local_file, name=filename, parent=synapse_parent), forceVersion=False)
                except Exception as ex:
                    exception = ex
                    self.log_error('[File ERROR] {0} -> {1} : {2}'.format(local_file, full_synapse_path, str(ex)))
                    if attempt_number < max_attempts:
                        sleep_time = random.randint(1, 5)
                        logging.info(
                            '[File RETRY in {0}s] {1} -> {2}'.format(sleep_time, local_file, full_synapse_path))
                        time.sleep(sleep_time)

            if exception:
                self.log_error('[File FAILED] {0} -> {1} : {2}'.format(local_file, full_synapse_path, str(exception)))
            else:
                synapse_file_md5 = synapse_file._file_handle['contentMd5']
                if local_md5 != synapse_file_md5:
                    self.log_error('Local MD5 does not match remote MD5 for: {0}'.format(local_file))

                self.add_processed_path(local_file)
                logging.info('[File UPLOADED] {0} -> {1}'.format(local_file, full_synapse_path))
        except Exception as ex:
            self.log_error('Error uploading file: {0}, {1}'.format(local_file, ex))

        return synapse_file

    def get_local_file_md5(self, filename):
        out_buffer = StringIO()
        sh.md5sum(filename, _out=out_buffer)
        local_file_md5 = out_buffer.getvalue().split()[0]
        return local_file_md5

    def set_synapse_parent(self, parent):
        with self._thread_lock:
            self._synapse_parents[parent.id] = parent

    def get_synapse_parent(self, parent_id):
        with self._thread_lock:
            return self._synapse_parents.get(parent_id, None)

    def get_synapse_path(self, folder_or_filename, parent):
        segments = []

        if isinstance(parent, Project):
            segments.insert(0, parent.name)
        else:
            next_parent = parent
            while next_parent:
                segments.insert(0, next_parent.name)
                next_parent = self.get_synapse_parent(next_parent.parentId)

        segments.append(folder_or_filename)

        return os.path.join(*segments)


class LogFilter(logging.Filter):
    FILTERS = [
        '##################################################',
        'Uploading file to Synapse storage',
        'Connection pool is full, discarding connection:'
    ]

    def filter(self, record):
        for filter in self.FILTERS:
            if filter in record.msg:
                return False
        return True


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            'csv', help='CSV file with GIT repository URLs to process.')
        parser.add_argument('-u', '--username',
                            help='Synapse username.', default=None)
        parser.add_argument('-p', '--password',
                            help='Synapse password.', default=None)
        parser.add_argument('-a', '--admin-team-id',
                            help='The Team ID to add to each Project.', default=None)
        parser.add_argument('-s', '--storage-location-id',
                            help='The Storage location ID for projects that are created.', default=None)
        parser.add_argument('-t', '--threads',
                            help='Set the maximum number of threads to run.', type=int, default=None)
        parser.add_argument('-w', '--work-dir', help='The directory to git pull repos into.', default=None)
        parser.add_argument('-l', '--log-level',
                            help='Set the logging level.', default='INFO')

        args = parser.parse_args()

        log_level = getattr(logging, args.log_level.upper())
        log_filename = 'log.txt'

        logging.basicConfig(
            filename=log_filename,
            filemode='w',
            format='%(asctime)s %(levelname)s: %(message)s',
            level=log_level
        )

        # Add console logging.
        console = logging.StreamHandler()
        console.setLevel(log_level)
        console.setFormatter(logging.Formatter('%(message)s'))
        logging.getLogger().addHandler(console)

        # Filter logs
        log_filter = LogFilter()
        for logger in [logging.getLogger(name) for name in logging.root.manager.loggerDict]:
            logger.addFilter(log_filter)

        # Silence sh logging
        logging.getLogger("sh").setLevel(logging.ERROR)

        GhapMigrator(
            args.csv,
            username=args.username,
            password=args.password,
            admin_team_id=args.admin_team_id,
            storage_location_id=args.storage_location_id,
            max_threads=args.threads,
            work_dir=args.work_dir
        ).start()
    except Exception:
        logging.exception('Unhandled exception.')


if __name__ == "__main__":
    main()
