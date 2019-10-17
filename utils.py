import os
import logging
import string

# This script needs to run in Python 3.4.
try:
    from os import scandir
except ImportError:
    from scandir import scandir


class Utils:

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
