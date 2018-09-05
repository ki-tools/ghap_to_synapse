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

    def __init__(self, csv_file_name, username=None, password=None):
        self._csv_file_name = csv_file_name
        self._username = username
        self._password = password
        self._work_dir = os.path.join(os.path.expanduser('~'), 'tmp', 'ghap')
        self._synapse_folders = {}
        self._git_to_syn_mappings = []

    def start(self):
        if(not os.path.exists(self._work_dir)):
            os.makedirs(self._work_dir)

        print('CSV File: {0}'.format(self._csv_file_name))
        print('Temp Directory: {0}'.format(self._work_dir))
        self.synapse_login()
        self.process_csv()

        print('Synapse Projects:')
        for line in self._git_to_syn_mappings:
            print(line)

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

    def process_csv(self):
        with open(self._csv_file_name) as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                git_url = row[0]
                self.migrate(git_url)

    def migrate(self, git_url):
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

        self.push_to_synapse(git_url, repo_name, repo_path)

    def push_to_synapse(self, git_url, repo_name, repo_path):
        project_name = 'GHAP - {0}'.format(repo_name)

        project = self.find_or_create_project(project_name)
        self._git_to_syn_mappings.append(
            '{0} -> {1}'.format(git_url, project.id))

        # Create the folders and upload the files.
        for dirpath, dirnames, filenames in os.walk(repo_path):

            if dirpath != repo_path:
                folder_name = dirpath.replace(repo_path + os.sep, '')

                # Do not process hidden folders, such as .git
                if (folder_name.startswith('.')):
                    continue

                print('Folder: {0}'.format(dirpath))
                full_synapse_path = os.path.join(project.id, folder_name)
                synapse_parent_path = os.path.dirname(full_synapse_path)
                synapse_parent = self.get_synapse_folder(synapse_parent_path)
                folder_name = os.path.basename(full_synapse_path)

                print('  -> {0}'.format(full_synapse_path))
                synapse_folder = self._synapse_client.store(
                    Folder(folder_name, parent=synapse_parent), forceVersion=False)
                self.set_synapse_folder(full_synapse_path, synapse_folder)

            for filename in filenames:
                # Do not process hidden folders, such as .gitignore
                if (filename.startswith('.')):
                    continue

                full_file_name = os.path.join(dirpath, filename)

                if (os.path.getsize(full_file_name) < 1):
                    continue

                print('File: {0}'.format(full_file_name))
                full_synapse_path = os.path.join(
                    project.id, full_file_name.replace(repo_path + os.sep, ''))
                synapse_parent_path = os.path.dirname(full_synapse_path)
                synapse_parent = self.get_synapse_folder(synapse_parent_path)

                print('  -> {0}'.format(full_synapse_path))
                synapse_file = self._synapse_client.store(
                    File(full_file_name, parent=synapse_parent), forceVersion=False)
                synapse_file_md5 = synapse_file._file_handle['contentMd5']

                out_buffer = StringIO()
                sh.md5sum(full_file_name, _out=out_buffer)
                local_file_md5 = out_buffer.getvalue().split()[0]
                if (local_file_md5 != synapse_file_md5):
                    raise Exception('Checksums do not match!')

    def find_or_create_project(self, project_name):
        profile = self._synapse_client.getUserProfile()

        query_str = """
            SELECT id, name
            FROM project
            WHERE project.createdByPrincipalId=={0}
            AND project.name=='{1}'
        """.format(profile['ownerId'], project_name)

        results = self._synapse_client.query(query_str)['results']

        project = None

        if (len(results)):
            project = self._synapse_client.get(
                Project(id=results[-1]['project.id']))
            print('Found Project: {0}: {1}'.format(project.id, project.name))
        else:
            project = self._synapse_client.store(Project(project_name))
            print('Created Project: {0}: {1}'.format(project.id, project.name))

        self.set_synapse_folder(project.id, project)

        return project

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

    args = parser.parse_args()

    GhapMigrator(
        args.csv, username=args.username, password=args.password
    ).start()


if __name__ == "__main__":
    main(sys.argv[1:])
