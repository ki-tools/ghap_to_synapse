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
import getpass
import asyncio
import aiohttp
import aiofiles
import synapseclient as syn
import random
from functools import partial
from utils import Utils
from aio_manager import AioManager


class SynapseProxy:
    _synapse_client = None
    login_error = None

    @classmethod
    def login(cls, username=None, password=None):
        cls._synapse_client = None
        cls.login_error = None

        username = username or os.getenv('SYNAPSE_USERNAME')
        password = password or os.getenv('SYNAPSE_PASSWORD')

        if not username:
            username = input('Synapse username: ')

        if not password:
            password = getpass.getpass(prompt='Synapse password: ')

        logging.info('Logging into Synapse as: {0}'.format(username))
        try:
            cls._synapse_client = syn.Synapse(skip_checks=True)
            cls._synapse_client.login(username, password, silent=True, rememberMe=False)
        except Exception as ex:
            cls._synapse_client = None
            cls.login_error = ex

        return cls._synapse_client is not None

    @classmethod
    def client(cls):
        if not cls._synapse_client:
            cls.login()
        return cls._synapse_client

    @classmethod
    def store(cls, obj, **kwargs):
        return cls.client().store(obj, **kwargs)

    @classmethod
    async def storeAsync(cls, obj, **kwargs):
        args = partial(cls.store, obj=obj, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(None, args)

    @classmethod
    def get(cls, entity, **kwargs):
        return cls.client().get(entity, **kwargs)

    @classmethod
    async def getAsync(cls, entity, **kwargs):
        args = partial(cls.get, entity=entity, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(None, args)

    @classmethod
    def getChildren(cls, parent, **kwargs):
        return list(cls.client().getChildren(parent, **kwargs))

    @classmethod
    async def getChildrenAsync(cls, parent, **kwargs):
        args = partial(cls.getChildren, parent=parent, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(None, args)

    @classmethod
    def tableQuery(cls, query, resultsAs="csv", **kwargs):
        return cls.client().tableQuery(query=query, resultsAs=resultsAs, **kwargs)

    @classmethod
    async def tableQueryAsync(cls, query, resultsAs="csv", **kwargs):
        args = partial(cls.tableQuery, query=query, resultsAs=resultsAs, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(None, args)

    @classmethod
    def delete(cls, obj, version=None):
        return cls.client().delete(obj, version=version)

    @classmethod
    async def deleteAsync(cls, obj, version=None):
        args = partial(cls.delete, obj=obj, version=version)
        return await asyncio.get_running_loop().run_in_executor(None, args)

    @classmethod
    def findEntityId(cls, name, parent=None):
        return cls.client().findEntityId(name, parent=parent)

    @classmethod
    async def findEntityIdAsync(cls, name, **kwargs):
        args = partial(cls.findEntityId, name=name, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(None, args)

    @classmethod
    def getFromFile(cls, md5):
        return cls.client().restGET('/entity/md5/%s' % md5)['results']

    @classmethod
    async def getFromFileAsync(cls, filepath, **kwargs):
        md5 = await Utils.get_local_file_md5(filepath)
        args = partial(cls.getFromFile, md5=md5, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(None, args)

    @classmethod
    async def find_project_by_name_or_id(cls, project_name_or_id, log_error_func):
        """Finds a Project by its name or ID.

        Args:
            project_name_or_id: The name or ID of the project to find.
            log_error_func: The error function.

        Returns:
            Project or None
        """
        project = None

        try:
            if project_name_or_id.lower().startswith('syn'):
                project = await cls.getAsync(project_name_or_id)
            else:
                project_id = await cls.findEntityIdAsync(project_name_or_id)
                project = await cls.getAsync(project_id)
        except syn.exceptions.SynapseHTTPError as ex:
            if ex.response.status_code >= 400:
                log_error_func('Script user does not have READ permission to Project: {0}'.format(project_name_or_id))
                return None
        except Exception as ex:
            # Project doesn't exist.
            pass

        return project

    class Aio:
        # File downloads have a max of 1 hour to download.
        FILE_DOWNLOAD_TIMEOUT = 60 * 60

        @classmethod
        async def rest_post(cls, url, endpoint=None, headers=None, body=None):
            max_attempts = 3
            attempt_number = 0

            while True:
                try:
                    uri, headers = SynapseProxy.client()._build_uri_and_headers(url, endpoint=endpoint, headers=headers)

                    if 'signature' in headers and isinstance(headers['signature'], bytes):
                        headers['signature'] = headers['signature'].decode("utf-8")

                    async with AioManager.AIOSESSION.post(uri, headers=headers, json=body) as response:
                        return await response.json()
                except Exception as ex:
                    logging.exception(ex)
                    attempt_number += 1
                    if attempt_number < max_attempts:
                        sleep_time = random.randint(1, 5)
                        logging.info('  Retrying POST in: {0}'.format(sleep_time))
                        await asyncio.sleep(sleep_time)
                    else:
                        logging.error('  Failed POST: {0}'.format(url))
                        raise

        @classmethod
        async def rest_get(cls, url, endpoint=None, headers=None, body=None):
            max_attempts = 3
            attempt_number = 0

            while True:
                try:
                    uri, headers = SynapseProxy.client()._build_uri_and_headers(url, endpoint=endpoint, headers=headers)

                    if 'signature' in headers and isinstance(headers['signature'], bytes):
                        headers['signature'] = headers['signature'].decode("utf-8")

                    async with AioManager.AIOSESSION.get(uri, headers=headers, json=body) as response:
                        return await response.json()
                except Exception as ex:
                    logging.exception(ex)
                    attempt_number += 1
                    if attempt_number < max_attempts:
                        sleep_time = random.randint(1, 5)
                        logging.info('  Retrying GET in: {0}'.format(sleep_time))
                        await asyncio.sleep(sleep_time)
                    else:
                        logging.error('  Failed GET: {0}'.format(url))
                        raise

        @classmethod
        async def download_file(cls, url, local_path, total_size):
            # TODO: Add resume ability for downloads.
            max_attempts = 3
            attempt_number = 0
            mb_total_size = Utils.pretty_size(total_size)

            while True:
                try:
                    timeout = aiohttp.ClientTimeout(total=cls.FILE_DOWNLOAD_TIMEOUT)

                    async with AioManager.AIOSESSION.get(url, timeout=timeout) as response:
                        async with aiofiles.open(local_path, mode='wb') as fd:
                            bytes_read = 0
                            while True:
                                chunk = await response.content.read(Utils.CHUNK_SIZE)
                                if not chunk:
                                    break
                                bytes_read += len(chunk)
                                Utils.print_inplace(
                                    'Saving {0} of {1}'.format(Utils.pretty_size(bytes_read), mb_total_size))

                                await fd.write(chunk)
                            Utils.print_inplace('')
                            logging.info('Saved {0}'.format(Utils.pretty_size(bytes_read)))
                            assert bytes_read == total_size
                            break
                except Exception as ex:
                    logging.exception(ex)
                    attempt_number += 1
                    if attempt_number < max_attempts:
                        sleep_time = random.randint(1, 5)
                        logging.error('  Retrying file in: {0}'.format(sleep_time))
                        await asyncio.sleep(sleep_time)
                    else:
                        logging.error('  Failed to download file: {0}'.format(local_path))
                        raise

        @classmethod
        async def get_children(cls,
                               parent,
                               includeTypes=["folder", "file", "table", "link", "entityview", "dockerrepo"],
                               sortBy="NAME",
                               sortDirection="ASC"):
            parent_id = parent
            if isinstance(parent, str):
                parent_id = parent
            elif isinstance(parent, syn.Entity):
                parent_id = parent.id
            elif isinstance(parent, dict):
                parent_id = parent['id']
            else:
                raise Exception('Invalid parent object: {0}'.format(parent))

            request = {
                'parentId': parent_id,
                'includeTypes': includeTypes,
                'sortBy': sortBy,
                'sortDirection': sortDirection,
                'includeTotalChildCount': True,
                'nextPageToken': None
            }

            response = {"nextPageToken": "first"}
            while response.get('nextPageToken') is not None:
                response = await cls.rest_post('/entity/children', body=request)
                for child in response['page']:
                    yield child
                request['nextPageToken'] = response.get('nextPageToken', None)

        @classmethod
        async def get_file_handle_id(cls, syn_id):
            request = {
                'includeEntity': True,
                'includeAnnotations': False,
                'includePermissions': False,
                'includeEntityPath': False,
                'includeHasChildren': False,
                'includeAccessControlList': False,
                'includeFileHandles': False,
                'includeTableBundle': False,
                'includeRootWikiId': False,
                'includeBenefactorACL': False,
                'includeDOIAssociation': False,
                'includeFileName': False,
                'includeThreadCount': False,
                'includeRestrictionInformation': False
            }

            res = await cls.rest_post('/entity/{0}/bundle2'.format(syn_id), body=request)

            return res.get('entity').get('name'), res.get('entity').get('dataFileHandleId')

        @classmethod
        async def get_filehandle(cls, syn_id, file_handle_id):
            body = {
                'includeFileHandles': True,
                'includePreSignedURLs': True,
                'includePreviewPreSignedURLs': False,
                'requestedFiles': [{
                    'fileHandleId': file_handle_id,
                    'associateObjectId': syn_id,
                    'associateObjectType': 'FileEntity'
                }]
            }

            res = await cls.rest_post('/fileHandle/batch',
                                      endpoint=SynapseProxy.client().fileHandleEndpoint,
                                      body=body)

            return res.get('requestedFiles', [])[0]

        @classmethod
        async def get_from_file(cls, filepath):
            md5 = await Utils.get_local_file_md5(filepath)
            res = await cls.rest_get('/entity/md5/{0}'.format(md5))
            return res.get('results')
