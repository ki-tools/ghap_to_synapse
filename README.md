# GHAP to Synapse Migration

A utility to migrate GIT repositories to Synapse.

## Usage

```shell
usage: ghap_migrator.py [-h] [-u USERNAME] [-p PASSWORD] [-s] csv

positional arguments:
  csv                   CSV file with GIT repository URLs to process.

optional arguments:
  -h, --help            show this help message and exit
  -u USERNAME, --username USERNAME
                        Synapse username.
  -p PASSWORD, --password PASSWORD
                        Synapse password.
  -s, --skip-md5        Skip md5 checks.
```
