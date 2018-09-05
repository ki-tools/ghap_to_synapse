# GHAP to Synapse Migration

A utility to migrate GIT repositories to Synapse.

## Dependencies

- [Python](https://www.python.org/)
- A [Synapse](https://www.synapse.org/) account with a username/password. Authentication through a 3rd party (.e.g., Google) will not work, you must have a Synapse user/pass for the [API to authenticate](http://docs.synapse.org/python/#connecting-to-synapse).
- synapseclient - Follow install instructions [here](http://docs.synapse.org/python/) or `(sudo) pip install (--upgrade) synapseclient[pandas,pysftp]`
- sh - `(sudo) pip install sh`

## Usage

```shell
usage: ghap_migrator.py [-h] [-u USERNAME] [-p PASSWORD] csv

positional arguments:
  csv                   CSV file with GIT repository URLs to process.

optional arguments:
  -h, --help            show this help message and exit
  -u USERNAME, --username USERNAME
                        Synapse username.
  -p PASSWORD, --password PASSWORD
                        Synapse password.
```
