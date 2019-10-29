import os
import logging
import getpass
import asyncio
import synapseclient as syn
from functools import partial


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
    def findEntityId(cls, name, parent=None):
        return cls.client().findEntityId(name, parent=parent)

    @classmethod
    async def findEntityIdAsync(cls, name, **kwargs):
        args = partial(cls.findEntityId, name=name, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(None, args)
