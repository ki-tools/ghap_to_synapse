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

import sys
import os
import argparse
import getpass
import sh
import tempfile
import csv
import synapseclient
from synapseclient import Project, Folder, File
from io import StringIO


class GhapMigrator:

    def __init__(self, csv_file_name, username=None, password=None, admin_team_id=None, storage_location_id=None, skip_md5=False):
        self._csv_file_name = csv_file_name
        self._username = username
        self._password = password
        self._admin_team_id = admin_team_id
        self._admin_team = None
        self._storage_location_id = storage_location_id
        self._storage_location = None
        self._skip_md5 = skip_md5
        self._work_dir = os.path.join(os.path.expanduser('~'), 'tmp', 'ghap')
        self._synapse_client = None
        self._script_user = None
        self._synapse_folders = {}
        self._git_to_syn_mappings = []
        self._errors = []

    def start(self):
        if (not os.path.exists(self._work_dir)):
            os.makedirs(self._work_dir)

        print('CSV File: {0}'.format(self._csv_file_name))
        print('Temp Directory: {0}'.format(self._work_dir))

        if self._skip_md5:
            print('Skipping MD5 Checks')

        self.synapse_login()
        self._script_user = self._synapse_client.getUserProfile()

        if self._admin_team_id != None and self._admin_team_id.strip() != '':
            print('Loading Admin Team ID: {0}'.format(self._admin_team_id))
            self._admin_team = self._synapse_client.getTeam(
                self._admin_team_id)
            print('Admin Team Loaded: {0}'.format(self._admin_team.name))
        else:
            self._admin_team_id = None

        if self._storage_location_id != None and self._storage_location_id.strip() != '':
            print('Loading Storage Location ID: {0}'.format(
                self._storage_location_id))
            self._storage_location = self._synapse_client.getMyStorageLocationSetting(
                self._storage_location_id)
            print('Storage Location: {0}'.format(
                self._storage_location['bucket']))
        else:
            self._storage_location_id = None

        self.process_csv()

        if len(self._git_to_syn_mappings) > 0:
            print('Synapse Projects:')
            for line in self._git_to_syn_mappings:
                print(' - {0}'.format(line))

        if len(self._errors) > 0:
            print('Completed with Errors:')
            for line in self._errors:
                print(' - {0}'.format(line))
        else:
            print('Completed Successfully.')

    def synapse_login(self):
        print('Logging into Synapse...')
        syn_user = self._username or os.getenv('SYNAPSE_USER')
        syn_pass = self._password or os.getenv('SYNAPSE_PASSWORD')

        if syn_user == None:
            syn_user = input('Synapse username: ')

        if syn_pass == None:
            syn_pass = getpass.getpass(prompt='Synapse password: ')

        self._synapse_client = synapseclient.Synapse()
        self._synapse_client.login(syn_user, syn_pass, silent=True)
        print('Logged in as: {0}'.format(syn_user))

    def process_csv(self):
        """
        Process the CSV file.
        The format CSV format is: "git_url,synapse_project_id"
          git_url:            The full GIT URL of the repository to migrate.
          synapse_project_id: The Synapse Project to migrate the repository into.
                              Blank = Create a new Project.
                              SynID = Use an existing Project and upload into a new Folder in the Project.
        """
        with open(self._csv_file_name) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            for row in reader:
                git_url = row['git_url']
                synapse_project_id = row['synapse_project_id']
                self.migrate(git_url, synapse_project_id)

    def migrate(self, git_url, synapse_project_id):
        print('Processing {0}'.format(git_url))

        repo_name = git_url.split('/')[-1].replace('.git', '')
        repo_path = os.path.join(self._work_dir, repo_name)

        if (os.path.exists(repo_path)):
            # Pull
            print(' Pulling Repo into {0}'.format(repo_path))
            sh.git.bake(_cwd=repo_path).pull()
        else:
            # Checkout
            print(' Checking Out into {0}'.format(repo_path))
            sh.git.bake(_cwd=self._work_dir).clone(git_url)

        self.push_to_synapse(git_url, repo_name, repo_path, synapse_project_id)

    def push_to_synapse(self, git_url, repo_name, repo_path, synapse_project_id):
        project = None

        starting_folder_name = ''

        if synapse_project_id != None and synapse_project_id != '':
            # Find or create a Folder in the Project to store the repo.
            project = self.find_or_create_project(synapse_project_id)
            if project == None:
                self._errors.append(
                    'ERROR: Could not get project for {0}.'.format(git_url))
                print(self._errors[-1])
                return

            full_synapse_path = os.path.join(project.id, repo_name)
            starting_folder = self.find_or_create_folder(
                repo_name, full_synapse_path)
            starting_folder_name = starting_folder.name
            self._git_to_syn_mappings.append(
                '{0} -> {1}'.format(git_url, full_synapse_path))
        else:
            # Find or Create the Project.
            project_name = 'GHAP - {0}'.format(repo_name)
            project = self.find_or_create_project(project_name)

            if project == None:
                self._errors.append(
                    'ERROR: Could not get project for {0}.'.format(git_url))
                print(self._errors[-1])
                return

            self._git_to_syn_mappings.append(
                '{0} -> {1}'.format(git_url, project.id))

        # Create the folders and upload the files.
        for dirpath, _, filenames in os.walk(repo_path):

            if dirpath != repo_path:
                folder_path = dirpath.replace(repo_path + os.sep, '')

                # Do not process hidden folders, such as .git
                if (folder_path.startswith('.')):
                    continue

                full_synapse_path = os.path.join(
                    project.id, starting_folder_name, folder_path)
                self.find_or_create_folder(dirpath, full_synapse_path)

            for filename in filenames:
                # Do not process hidden folders, such as .gitignore
                if (filename.startswith('.')):
                    continue

                full_file_name = os.path.join(dirpath, filename)

                # Delete any existing git log files and skip the file since it will be recreated below.
                if full_file_name.endswith('.gitlog'):
                    os.remove(full_file_name)
                    continue

                # Skip empty files since these will error when uploading via the synapseclient.
                if (os.path.getsize(full_file_name) < 1):
                    continue

                # Get the GIT log for the file.
                git_log_file_name = os.path.join(
                    dirpath, '{0}.gitlog'.format(filename))
                sh.git.bake('--no-pager', _cwd=dirpath).log(filename,
                                                            _out=git_log_file_name, _tty_out=False)

                for upload_file_name in [full_file_name, git_log_file_name]:
                    full_synapse_path = os.path.join(
                        project.id, starting_folder_name, upload_file_name.replace(repo_path + os.sep, ''))
                    self.find_or_upload_file(
                        upload_file_name, full_synapse_path)

    def find_or_create_project(self, project_name_or_id):
        project = None

        try:
            if project_name_or_id.lower().startswith('syn'):
                project = self._synapse_client.get(project_name_or_id)
            else:
                project_id = self._synapse_client.findEntityId(
                    project_name_or_id)
                project = self._synapse_client.get(project_id)
        except synapseclient.exceptions.SynapseHTTPError as ex:
            if ex.response.status_code >= 400:
                self._errors.append(
                    'ERROR: Script user does not have READ permission to Project: {0}'.format(project_name_or_id))
                print(self._errors[-1])
                return None
        except Exception as ex:
            # Project doesn't exist.
            pass

        if project:
            print('Found Project: {0}: {1}'.format(project.id, project.name))
            if not self.has_write_permissions(project):
                self._errors.append(
                    'ERROR: Script user does not have WRITE permission to Project: {0}'.format(project_name_or_id))
                print(self._errors[-1])
                return None
        else:
            project = self._synapse_client.store(Project(project_name_or_id))
            print('Created Project: {0}: {1}'.format(project.id, project.name))
            if self._storage_location_id:
                print('Setting storage location for project.')
                self._synapse_client.setStorageLocation(
                    project, self._storage_location_id)

            if self._admin_team:
                print('Granting admin permissions to team.')
                self.grant_admin_access(project, self._admin_team.id)

        if project:
            self.set_synapse_folder(project.id, project)

        return project

    def has_write_permissions(self, project):
        # Check for user specific permissions.
        user_perms = set(self._synapse_client.getPermissions(
            project, self._script_user.ownerId))
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
        self._synapse_client.setPermissions(
            project, grantee_id, accessType=accessType, warn_if_inherits=False)

    def find_or_create_folder(self, folder_path, full_synapse_path):
        folder = None

        print('Folder: {0}'.format(folder_path))
        synapse_parent_path = os.path.dirname(full_synapse_path)
        synapse_parent = self.get_synapse_folder(synapse_parent_path)
        print('  -> {0}'.format(full_synapse_path))
        folder_name = os.path.basename(full_synapse_path)

        syn_folder_id = self._synapse_client.findEntityId(
            folder_name, parent=synapse_parent)

        if syn_folder_id:
            folder = self._synapse_client.get(
                syn_folder_id, downloadFile=False)
            print('  -> Folder Already Exists.')
        else:
            folder = self._synapse_client.store(
                Folder(name=folder_name, parent=synapse_parent))
            print('  -> Folder Created.')

        self.set_synapse_folder(full_synapse_path, folder)

        return folder

    def find_or_upload_file(self, full_file_name, full_synapse_path):
        print('File: {0}'.format(full_file_name))
        synapse_parent_path = os.path.dirname(full_synapse_path)
        synapse_parent = self.get_synapse_folder(synapse_parent_path)
        print('  -> {0}'.format(full_synapse_path))
        file_name = os.path.basename(full_synapse_path)

        file = None
        needs_upload = True

        if not self._skip_md5:
            # Check if the file has already been uploaded and has not changed since being uploaded.
            syn_file_id = self._synapse_client.findEntityId(
                file_name, parent=synapse_parent)

            if syn_file_id:
                file = self._synapse_client.get(
                    syn_file_id, downloadFile=False)

                synapse_file_md5 = file._file_handle['contentMd5']
                local_md5 = self.get_local_file_md5(full_file_name)
                if (local_md5 == synapse_file_md5):
                    needs_upload = False
                    print('  -> File Already Uploaded.')
                else:
                    file = None
                    print('  -> File Already Uploaded but has changes.')

        if needs_upload:
            file = self._synapse_client.store(
                File(full_file_name, parent=synapse_parent))
            print('  -> File Uploaded.')

        return file

    def get_local_file_md5(self, file_name):
        out_buffer = StringIO()
        sh.md5sum(file_name, _out=out_buffer)
        local_file_md5 = out_buffer.getvalue().split()[0]
        return local_file_md5

    def get_synapse_folder(self, synapse_path):
        return self._synapse_folders[synapse_path]

    def set_synapse_folder(self, synapse_path, parent):
        self._synapse_folders[synapse_path] = parent


def main(argv):
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
    parser.add_argument('-m', '--skip-md5', help='Skip md5 checks.',
                        default=False, action='store_true')

    args = parser.parse_args()

    GhapMigrator(
        args.csv,
        username=args.username,
        password=args.password,
        admin_team_id=args.admin_team_id,
        storage_location_id=args.storage_location_id,
        skip_md5=args.skip_md5
    ).start()


if __name__ == "__main__":
    main(sys.argv[1:])
