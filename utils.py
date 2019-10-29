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
import string
import getpass
import csv
import sh
import shutil
import synapseclient
import urllib.parse as UrlParse

# This script needs to run in Python 3.4.
try:
    from os import scandir
except ImportError:
    from scandir import scandir


class Utils:

    @staticmethod
    def setup_logging(filename, level=None):
        logging.basicConfig(
            filename=filename,
            filemode='w',
            format='%(asctime)s %(levelname)s: %(message)s',
            level=level
        )

        # Add console logging.
        console = logging.StreamHandler()
        console.setLevel(level)
        console.setFormatter(logging.Formatter('%(message)s'))
        logging.getLogger().addHandler(console)

        # Filter logs
        log_filter = LogFilter()
        for logger in [logging.getLogger(name) for name in logging.root.manager.loggerDict]:
            logger.addFilter(log_filter)

        # Silence sh logging
        logging.getLogger("sh").setLevel(logging.ERROR)

    @staticmethod
    def synapse_login(username, password):
        logging.info('Logging into Synapse...')
        username = username or os.getenv('SYNAPSE_USERNAME')
        password = password or os.getenv('SYNAPSE_PASSWORD')

        if not username:
            username = input('Synapse username: ')

        if not password:
            password = getpass.getpass(prompt='Synapse password: ')

        try:
            synapse_client = synapseclient.Synapse(skip_checks=True)
            synapse_client.login(username, password, silent=True, rememberMe=False)
            return synapse_client, None
        except Exception as ex:
            return None, str(ex)

    @staticmethod
    def process_repo_csv(csv_filename, work_dir, success_func, error_func):
        git_lfs_installed = Utils.git_lfs_installed()

        for row in Utils.csv_repo_reader(csv_filename):
            git_url = row['git_url']
            git_folder = row['git_folder']
            synapse_project_id = row['synapse_project_id']
            synapse_path = row['synapse_path']

            logging.info('=' * 80)
            logging.info('Processing {0}'.format(git_url))
            if git_folder:
                logging.info('  - Folder: {0}'.format(git_folder))

            repo_url_path, repo_name, repo_local_path = Utils.parse_git_url(git_url, work_dir)

            git_errors = Utils.get_git_repo(git_url,
                                            repo_local_path,
                                            work_dir,
                                            git_lfs_installed=git_lfs_installed)

            if git_errors:
                for error in git_errors:
                    error_func(error)
            else:
                success_func(git_url, repo_name, repo_local_path, git_folder, synapse_project_id, synapse_path)

    @staticmethod
    def csv_repo_reader(csv_filename):
        """Reads a CSV file and yields the properties of each row as a dict.

        The CSV format is: "git_url,git_folder,synapse_project_id,synapse_path"
          git_url:            The full GIT URL of the repository to migrate.
          git_folder:         The folder within the repo to migrate.
                              Blank = Migrate the whole repo.
                              Name = Only migrate the specific folder.
          synapse_project_id: The Synapse Project to migrate the repository into.
                              Blank = Create a new Project.
                              SynID = Use an existing Project and upload into a new Folder in the Project.
          synapse_path:       The folder path in Synapse to store the files (e.g., EDD/common)

        Args:
            csv_filename:

        Returns:

        """
        with open(csv_filename) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            for row in reader:
                yield {
                    'git_url': row['git_url'].strip(),
                    'git_folder': row['git_folder'].strip(),
                    'synapse_project_id': row['synapse_project_id'].strip(),
                    'synapse_path': row['synapse_path'].lstrip(os.sep).rstrip(os.sep)
                }

    @staticmethod
    def parse_git_url(git_url, work_dir):
        repo_url_path = UrlParse.urlparse(git_url).path.replace('.git', '').lstrip('/')
        repo_name = repo_url_path.split('/')[-1]
        repo_local_path = os.path.join(work_dir, repo_url_path)
        return repo_url_path, repo_name, repo_local_path

    @staticmethod
    def get_git_repo(git_url, repo_path, work_dir, git_lfs_installed=False):
        errors = []

        # Use 'lfs' for git commands to get around memory constraints when using
        # the normal 'git clone'/'git pull' commands.
        # https://github.com/git-lfs/git-lfs/issues/3524
        lfs = 'lfs' if git_lfs_installed else ''

        if os.path.exists(repo_path):
            # Pull
            logging.info('  - Pulling Repo into {0}'.format(repo_path))
            try:
                if lfs:
                    sh.git.bake(_cwd=repo_path).lfs('pull')
                else:
                    sh.git.bake(_cwd=repo_path).pull()
            except Exception as ex:
                errors.append('Error pulling repo: {0} : {1}'.format(git_url, ex))
        else:
            # Checkout
            logging.info('  - Cloning into {0}'.format(repo_path))
            try:
                if lfs:
                    sh.git.bake(_cwd=work_dir).lfs('clone', git_url, repo_path)
                else:
                    sh.git.bake(_cwd=work_dir).clone(git_url, repo_path)
            except Exception as ex:
                errors.append('Error cloning repo: {0} : {1}'.format(git_url, ex))
                if os.path.isdir(repo_path):
                    shutil.rmtree(repo_path)

                # Try alternate cloning
                if lfs:
                    logging.info('Trying alternate git clone...')
                    try:
                        sh.git.bake(_cwd=work_dir).lfs('clone', '--depth', '1', git_url, repo_path)
                        sh.git.bake(_cwd=repo_path).fetch('--unshallow')
                        sh.git.bake(_cwd=repo_path).config('remote.origin.fetch', '+refs/heads/*:refs/remotes/origin/*')
                        sh.git.bake(_cwd=repo_path).fetch('origin')
                    except Exception as ex2:
                        errors.append('Error cloning repo: {0} : {1}'.format(git_url, ex2))
                        if os.path.isdir(repo_path):
                            shutil.rmtree(repo_path)

        return errors

    @staticmethod
    def git_lfs_installed():
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

    @staticmethod
    def create_project_name(repo_name, git_folder=None):
        project_name = 'GHAP - {0}'.format(repo_name)
        if git_folder:
            project_name += ' - {0}'.format(git_folder.replace('/', '-'))
        return project_name

    @staticmethod
    def expand_path(local_path):
        var_path = os.path.expandvars(local_path)
        expanded_path = os.path.expanduser(var_path)
        return os.path.abspath(expanded_path)

    @staticmethod
    def get_dirs_and_files(local_path):
        dirs = []
        files = []

        for entry in scandir(local_path):
            if entry.is_dir(follow_symlinks=False):
                # Do not include .git
                if os.path.basename(entry.path) == '.git':
                    logging.info('Skipping GIT Directory: {0}'.format(entry.path))
                    continue

                dirs.append(entry)
            else:
                # Skip the *.gitlog files since they will be created during upload.
                if os.path.basename(entry.path).endswith('.gitlog'):
                    continue

                files.append(entry)

        dirs.sort(key=lambda f: f.name)
        files.sort(key=lambda f: f.name)

        return dirs, files

    VALID_FILENAME_CHARS = frozenset("-_.() %s%s" % (string.ascii_letters, string.digits))

    @staticmethod
    def get_invalid_synapse_name_chars(name):
        """Returns any invalid characters (for Synapse) from a string.

        Args:
            name: The string to check.

        Returns:
            List of invalid characters.
        """
        bad_chars = [c for c in name if c not in Utils.VALID_FILENAME_CHARS]
        return bad_chars

    @staticmethod
    def get_path_parts(path):
        return filter(None, os.path.normpath(path).split(os.sep))


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
