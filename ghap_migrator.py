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
import time
import datetime
import random
import synapseclient
import asyncio
import csv
from synapseclient import Project, Folder, File
from utils import Utils
from aio_manager import AioManager
from synapse_proxy import SynapseProxy


class GhapMigrator:

    def __init__(self, csv_filename, username=None, password=None, admin_team_id=None, storage_location_id=None,
                 work_dir=None, git_pull_only=False, timestamp=None):
        self._csv_filename = csv_filename
        self._username = username
        self._password = password
        self._admin_team_id = admin_team_id
        self._admin_team = None
        self._storage_location_id = storage_location_id
        self._storage_location = None
        self._work_dir = None
        self._git_pull_only = git_pull_only
        self._timestamp = timestamp

        if work_dir is None:
            self._work_dir = Utils.expand_path(os.path.join('~', 'tmp', 'ghap'))
        else:
            self._work_dir = Utils.expand_path(work_dir)

        self._script_user = None
        self._synapse_parents = {}
        self._git_to_syn_mappings = []
        self._stats = {
            'found': [],
            'processed': []
        }
        self._errors = []
        self._start_time = None
        self._end_time = None

    def log_error(self, msg):
        """
        Logs the error message and adds it to the errors list.
        """
        # Do not add duplicate errors.
        if msg not in self._errors:
            self._errors.append(msg)
        logging.error(msg)

    def add_processed_path(self, path):
        """
        Adds a file/folder path to the stats.
        This is used to verify each local file is uploaded to Synapse.
        """
        self._stats['processed'].append(path)

    def start(self):
        self._start_time = time.time()
        if not os.path.exists(self._work_dir):
            os.makedirs(self._work_dir)

        Utils.git_lfs_installed()

        logging.info("Started at: {0}".format(datetime.datetime.now()))
        logging.info('CSV File: {0}'.format(self._csv_filename))
        logging.info('Work Directory: {0}'.format(self._work_dir))

        if not SynapseProxy.login(self._username, self._password):
            self.log_error('Synapse login failed: {0}'.format(SynapseProxy.login_error))
        else:
            AioManager.start(self._startAsync)
            self.flush_csv_lines()

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

        if self.WRITE_CSV_LINES_FILE:
            logging.info('Processed CSV File: {0}'.format(self.WRITE_CSV_LINES_FILE))

    async def _startAsync(self):
        self._script_user = SynapseProxy.client().getUserProfile()

        if self._admin_team_id and self._admin_team_id.strip() != '':
            logging.info('Loading Admin Team ID: {0}'.format(self._admin_team_id))
            self._admin_team = SynapseProxy.client().getTeam(self._admin_team_id)
            logging.info('Admin Team Loaded: {0}'.format(self._admin_team.name))
        else:
            self._admin_team_id = None

        if self._storage_location_id and self._storage_location_id.strip() != '':
            logging.info('Loading Storage Location ID: {0}'.format(self._storage_location_id))
            self._storage_location = SynapseProxy.client().getMyStorageLocationSetting(self._storage_location_id)
            logging.info('Storage Location: {0}'.format(self._storage_location['bucket']))
        else:
            self._storage_location_id = None

        if self._git_pull_only:
            logging.info('Action: git clone/pull only')
            await Utils.process_repo_csv(self._csv_filename, self._work_dir, self.repo_pulled, self.log_error)
        else:
            logging.info('Action: migrate')
            await Utils.process_repo_csv(self._csv_filename, self._work_dir, self.push_to_synapse, self.log_error)

    async def repo_pulled(self, git_url, repo_name, repo_path, git_folder, synapse_project_id, synapse_path):
        # noop
        pass

    async def push_to_synapse(self, git_url, repo_name, repo_path, git_folder, synapse_project_id, synapse_path):
        project = None

        if synapse_project_id and synapse_project_id != '':
            # Find or create a Folder in the Project to store the repo.
            project = await self.find_or_create_project(synapse_project_id)
        else:
            # Find or Create the Project.
            project_name = Utils.build_project_name(repo_name, git_folder)
            project = await self.find_or_create_project(project_name)

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
            for folder in Utils.get_path_parts(synapse_path):
                full_path = os.path.join(full_path, folder)
                parent = await self.find_or_create_folder(full_path, parent, is_remote_only=True)

        start_path = repo_path
        if git_folder:
            start_path = os.path.join(repo_path, git_folder)

        await self.upload_folder(start_path, parent)

    async def upload_folder(self, local_path, synapse_parent):
        try:
            if not synapse_parent:
                self.log_error('Parent not found, cannot upload folder: {0}'.format(local_path))
                return

            parent = synapse_parent

            dirs, files = Utils.get_dirs_and_files(local_path)
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

                await self.find_or_upload_file(file_entry.path, parent)

            # Upload the directories.
            for dir_entry in dirs:
                syn_dir = await self.find_or_create_folder(dir_entry.path, parent, is_remote_only=False)
                await self.upload_folder(dir_entry.path, syn_dir)
        except Exception as ex:
            self.log_error('Error uploading folder: {0}, {1}'.format(local_path, ex))

    async def find_or_create_project(self, project_name_or_id):
        project = await SynapseProxy.find_project_by_name_or_id(project_name_or_id, self.log_error)

        if project:
            logging.info('[Project FOUND] {0}: {1}'.format(project.id, project.name))
            if not self.has_write_permissions(project):
                self.log_error('Script user does not have WRITE permission to Project: {0}'.format(project_name_or_id))
                project = None
        else:
            try:
                bad_name_chars = Utils.get_invalid_synapse_name_chars(project_name_or_id)
                if bad_name_chars:
                    self.log_error(
                        'Project name: "{0}" contains invalid characters: "{1}"'.format(project_name_or_id,
                                                                                        ''.join(bad_name_chars)))
                else:
                    project = await SynapseProxy.storeAsync(Project(project_name_or_id))
                    logging.info('[Project CREATED] {0}: {1}'.format(project.id, project.name))
                    if self._storage_location_id:
                        logging.info('Setting storage location for project: {0}: {1}'.format(project.id, project.name))
                        SynapseProxy.client().setStorageLocation(project, self._storage_location_id)

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
        user_perms = set(SynapseProxy.client().getPermissions(project, self._script_user.ownerId))
        if ('CREATE' in user_perms) and ('UPDATE' in user_perms):
            return True

        # Check for team permissions.
        acl = SynapseProxy.client()._getACL(project)

        for resourceAccess in acl['resourceAccess']:
            principalId = resourceAccess['principalId']
            try:
                team = SynapseProxy.client().getTeam(principalId)
                team_members = SynapseProxy.client().getTeamMembers(team)
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
        SynapseProxy.client().setPermissions(project, grantee_id, accessType=accessType, warn_if_inherits=False)

    async def find_or_create_folder(self, path, synapse_parent, is_remote_only=False):
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

        syn_folder_id = await SynapseProxy.findEntityIdAsync(folder_name, parent=synapse_parent)

        if syn_folder_id:
            synapse_folder = await SynapseProxy.getAsync(syn_folder_id, downloadFile=False)
            self.set_synapse_parent(synapse_folder)
            self.add_processed_path(path)
            self.write_csv_line(path, full_synapse_path, synapse_folder.id, is_remote_only=is_remote_only)
            logging.info('[Folder EXISTS]: {0} -> {1}'.format(path, full_synapse_path))
        else:
            max_attempts = 5
            attempt_number = 0
            exception = None

            while attempt_number < max_attempts and not synapse_folder:
                try:
                    attempt_number += 1
                    exception = None
                    synapse_folder = await SynapseProxy.storeAsync(
                        Folder(name=folder_name, parent=synapse_parent), forceVersion=False)
                except Exception as ex:
                    exception = ex
                    self.log_error('[Folder ERROR] {0} -> {1} : {2}'.format(path, full_synapse_path, str(ex)))
                    if attempt_number < max_attempts:
                        sleep_time = random.randint(1, 5)
                        logging.info('[Folder RETRY in {0}s] {1} -> {2}'.format(sleep_time, path, full_synapse_path))
                        asyncio.sleep(sleep_time)

            if exception:
                self.log_error('[Folder FAILED] {0} -> {1} : {2}'.format(path, full_synapse_path, str(exception)))
            else:
                self.add_processed_path(path)
                self.write_csv_line(path, full_synapse_path, synapse_folder.id, is_remote_only=is_remote_only)
                logging.info('[Folder CREATED] {0} -> {1}'.format(path, full_synapse_path))
                self.set_synapse_parent(synapse_folder)

        return synapse_folder

    async def find_or_upload_file(self, local_file, synapse_parent):
        synapse_file = None
        try:
            if not synapse_parent:
                self.log_error('Parent not found, cannot upload file: {0}'.format(local_file))
                return synapse_file

            if os.path.getsize(local_file) < 1:
                logging.info('Skipping Empty File: {0}'.format(local_file))
                self.add_processed_path(local_file)
                self.write_csv_line(local_file, '-empty-file-not-uploaded-', '-')
                return synapse_file

            filename = os.path.basename(local_file)

            bad_name_chars = Utils.get_invalid_synapse_name_chars(filename)
            if bad_name_chars:
                self.log_error(
                    'File name: "{0}" contains invalid characters: "{1}"'.format(local_file, ''.join(bad_name_chars)))
                return synapse_file

            full_synapse_path = self.get_synapse_path(filename, synapse_parent)

            # Check if the file has already been uploaded and has not changed since being uploaded.
            syn_file_id = await SynapseProxy.findEntityIdAsync(filename, parent=synapse_parent)

            # Synapse trims the "Synapse Name" of a file so if the file begins or ends with a space
            # try and find the entity with the spaces removed.
            if not syn_file_id and (filename.startswith(' ') or filename.endswith(' ')):
                syn_file_id = await SynapseProxy.findEntityIdAsync(filename.strip(), parent=synapse_parent)

            local_md5 = await Utils.get_local_file_md5(local_file)

            if syn_file_id:
                synapse_file = await SynapseProxy.getAsync(syn_file_id, downloadFile=False)
                synapse_file_md5 = synapse_file._file_handle['contentMd5']
                synapse_file_name = synapse_file._file_handle['fileName']

                # Make sure the actual file name matches.
                if synapse_file_name != filename:
                    raise Exception(
                        'Synapse file name: {0} ({1}) does not match local file name: {2}'.format(synapse_file_name,
                                                                                                  synapse_file.id,
                                                                                                  local_file))

                if local_md5 == synapse_file_md5:
                    logging.info('[File is CURRENT] {0} -> {1}'.format(local_file, full_synapse_path))
                    self.add_processed_path(local_file)
                    self.write_csv_line(local_file, full_synapse_path, synapse_file.id)
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
                    synapse_file = await SynapseProxy.storeAsync(
                        File(path=local_file, name=filename, parent=synapse_parent), forceVersion=False)
                except Exception as ex:
                    exception = ex
                    self.log_error('[File ERROR] {0} -> {1} : {2}'.format(local_file, full_synapse_path, str(ex)))
                    if attempt_number < max_attempts:
                        sleep_time = random.randint(1, 5)
                        logging.info(
                            '[File RETRY in {0}s] {1} -> {2}'.format(sleep_time, local_file, full_synapse_path))
                        asyncio.sleep(sleep_time)

            # Verify the file uploaded successfully.
            if synapse_file and exception is None:
                local_size = os.path.getsize(local_file)
                remote_md5 = synapse_file._file_handle['contentMd5']
                remote_size = synapse_file._file_handle['contentSize']

                if local_md5 != remote_md5:
                    exception = Exception('Local MD5 does not match remote MD5 for: {0}'.format(local_file))

                if local_size != remote_size:
                    exception = Exception('Local size: {0} does not match remote size: {1} for: {2}'.format(local_size,
                                                                                                            remote_size,
                                                                                                            local_file))

            if exception:
                self.log_error('[File FAILED] {0} -> {1} : {2}'.format(local_file, full_synapse_path, str(exception)))
            else:
                self.add_processed_path(local_file)
                self.write_csv_line(local_file, full_synapse_path, synapse_file.id)
                logging.info('[File UPLOADED] {0} -> {1}'.format(local_file, full_synapse_path))
        except Exception as ex:
            self.log_error('Error uploading file: {0}, {1}'.format(local_file, ex))

        return synapse_file

    def set_synapse_parent(self, parent):
        self._synapse_parents[parent.id] = parent

    def get_synapse_parent(self, parent_id):
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

    WRITE_CSV_LINES_FILE = None
    WRITE_CSV_LINES_HEADERS = ['local_path', 'remote_path', 'synapse_id', 'is_remote_only']
    WRITE_CSV_LINES_BUFFER = []

    def write_csv_line(self, local_path, remote_path, synapse_id, is_remote_only=False):
        self.WRITE_CSV_LINES_BUFFER.append({
            'local_path': local_path,
            'remote_path': remote_path,
            'synapse_id': synapse_id,
            'is_remote_only': is_remote_only
        })

        if len(self.WRITE_CSV_LINES_BUFFER) >= 5000 or len(self.WRITE_CSV_LINES_BUFFER) == 1:
            self.flush_csv_lines()

    def flush_csv_lines(self):
        is_new = False

        if self.WRITE_CSV_LINES_FILE is None:
            is_new = True
            self.WRITE_CSV_LINES_FILE = 'ghap_migrator_processed_{0}.csv'.format(self._timestamp)

        with open(self.WRITE_CSV_LINES_FILE, mode='a+') as csv_file:
            writer = csv.DictWriter(csv_file,
                                    delimiter=',',
                                    quotechar='"',
                                    fieldnames=self.WRITE_CSV_LINES_HEADERS,
                                    quoting=csv.QUOTE_ALL)
            if is_new:
                writer.writeheader()

            for obj in self.WRITE_CSV_LINES_BUFFER:
                writer.writerow(obj)

        self.WRITE_CSV_LINES_BUFFER.clear()


def main():
    try:
        parser = argparse.ArgumentParser(description='Migrates GIT repos from GHAP to Synapse.')
        parser.add_argument(
            'csv', help='CSV file with GIT repository URLs to process.')
        parser.add_argument('-u', '--username', help='Synapse username.', default=None)
        parser.add_argument('-p', '--password', help='Synapse password.', default=None)
        parser.add_argument('-a', '--admin-team-id', help='The Team ID to add to each Project.', default=None)
        parser.add_argument('-s', '--storage-location-id',
                            help='The Storage location ID for projects that are created.', default=None)
        parser.add_argument('-w', '--work-dir', help='The directory to git pull repos into.', default=None)
        parser.add_argument('-l', '--log-level', help='Set the logging level.', default='INFO')
        parser.add_argument('-g', '--git-pull-only',
                            help='Only git pull the repos in the CSV file.',
                            default=False,
                            action='store_true')

        args = parser.parse_args()

        log_level = getattr(logging, args.log_level.upper())
        timestamp = Utils.timestamp_str()
        log_filename = 'ghap_migrator_log_{0}.txt'.format(timestamp)
        Utils.setup_logging(log_filename, log_level)

        GhapMigrator(
            args.csv,
            username=args.username,
            password=args.password,
            admin_team_id=args.admin_team_id,
            storage_location_id=args.storage_location_id,
            work_dir=args.work_dir,
            git_pull_only=args.git_pull_only,
            timestamp=timestamp
        ).start()
    except Exception as ex:
        logging.exception('Unhandled exception: {0}'.format(ex))


if __name__ == "__main__":
    main()
