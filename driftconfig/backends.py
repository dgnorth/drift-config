# -*- coding: utf-8 -*-
'''
ReLib Backends
'''
import logging
import os
import StringIO
from urlparse import urlparse

from .relib import Backend, BackendError, register

log = logging.getLogger(__name__)


@register
class S3Backend(Backend):
    """
    S3 backend for TableStore.
    """

    __scheme__ = 's3'

    def __init__(self, bucket_name, folder_name, region_name=None):
        import boto3
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.region_name = region_name

    @classmethod
    def create_from_url_parts(cls, parts, query):
        if 'region' in query:
            region_name = query['region'][0]
        else:
            region_name = None
        return cls(bucket_name=parts.hostname, folder_name=parts.path, region_name=region_name)

    def get_url(self):
        url = 's3://{}/{}'.format(self.bucket_name, self.folder_name)
        if self.region_name:
            url += '?region=' + self.region_name
        return url

    def __str__(self):
        return "S3Backend's3://{}/{}'".format(self.bucket_name, self.folder_name)

    def get_key_name(self, file_name):
        return '{}/{}'.format(self.folder_name, file_name)

    def save_data(self, file_name, data):
        f = StringIO.StringIO(data)
        key_name = self.get_key_name(file_name)
        self.on_progress("Uploading {} bytes to s3://{}/{}".format(len(data), self.bucket_name, key_name))
        self.s3_client.upload_fileobj(
            f,
            self.bucket_name,
            key_name,
            ExtraArgs={'ContentType': 'application/json'},
        )

    def load_data(self, file_name):
        key_name = self.get_key_name(file_name)
        self.on_progress("Downloading s3://{}/{}".format(self.bucket_name, key_name))
        f = StringIO.StringIO()
        self.s3_client.download_fileobj(self.bucket_name, key_name, f)
        return f.getvalue()


@register
class RedisBackend(Backend):

    __scheme__ = 'redis'

    def __init__(self, host=None, port=None, db=None, prefix=None, expire_sec=None):
        import redis
        host = host or 'localhost'
        port = port or 6379
        db = db or 0
        self.prefix = prefix or ''
        self.expire_sec = expire_sec

        self.conn = redis.StrictRedis(
            host=host,
            port=port,
            db=db,
        )

        self.host, self.port, self.db = host, port, db

    @classmethod
    def create_from_url_parts(cls, parts, query):
        db = int(parts.path) if parts.path else None
        prefix = query['prefix'][0] if 'prefix' in query else None
        expire_sec = query['expire_sec'][0] if 'expire_sec' in query else None
        return cls(host=parts.hostname, port=parts.port, db=db, prefix=prefix, expire_sec=expire_sec)

    def __str__(self):
        return "RedisBackend'{}:{}#{}'".format(self.host, self.port, self.db)

    def get_key_name(self, file_name):
        return 'relib:{}:{}'.format(self.prefix, file_name)

    def save_data(self, file_name, data):
        key_name = self.get_key_name(file_name)
        self.on_progress("Adding {} bytes to Redis:{}".format(len(data), key_name))
        self.conn.set(key_name, data)
        if self.expire_sec is not None:
            self.conn.expire(key_name, self.expire_sec)
        self.conn.set

    def load_data(self, file_name):
        key_name = self.get_key_name(file_name)
        self.on_progress("Reading from Redis:{}".format(key_name))
        data = self.conn.get(key_name)
        if data is None:
            raise BackendError("Redis cache doesn't have '{}'".format(key_name))
        return data


@register
class FileBackend(Backend):

    __scheme__ = 'file'

    def __init__(self, folder_name):
        if '~' in folder_name:
            # Expand user and trim whatever was in front of the ~ char.
            folder_name = os.path.expanduser('~') + folder_name.split('~', 1)[1]

        folder_name = folder_name.replace('/', os.sep)  # Adjust to Windows platform mainly

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        self.folder_name = folder_name

    @classmethod
    def create_from_url_parts(cls, parts, query):
        # combine host and path into one
        path = parts.netloc or ''  # Change None to '' if needed.
        path += parts.path
        return cls(folder_name=path)

    def get_url(self):
        path = self.folder_name
        path = path.replace(os.path.expanduser('~'), '~')
        path = path.replace('\\', '/')  # De-Windowize, if needed
        return path

    def __str__(self):
        return "FileBackend'{}'".format(self.folder_name)

    def get_filename(self, file_name):
        file_name = file_name.replace('/', os.sep)  # Adjust to Windows platform mainly
        return os.path.join(self.folder_name, file_name)

    def save_data(self, file_name, data):
        path_name = self.get_filename(file_name)

        # Create subdirs if neccessary
        dir_name = os.path.dirname(path_name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        with open(path_name, 'w') as f:
            self.on_progress("Writing {} bytes to {}".format(len(data), path_name))
            f.write(data)

    def load_data(self, file_name):
        path_name = self.get_filename(file_name)
        self.on_progress("Reading from {}".format(path_name))
        with open(path_name, 'r') as f:
            return f.read()


@register
class MemoryBackend(Backend):

    __scheme__ = 'memory'
    archive = {}

    def __init__(self, folder_name):
        self.folder_name = folder_name

    def __del__(self):
        del MemoryBackend.archive[self.folder_name]

    @classmethod
    def create_from_url_parts(cls, parts, query):
        return cls(folder_name=parts.path)

    def get_url(self):
        return 'memory://' + self.folder_name

    def __str__(self):
        return "MemoryBackend'{}'".format(self.folder_name)

    def save_data(self, file_name, data):
        MemoryBackend.archive[self.folder_name].setdefault({})[file_name] = data

    def load_data(self, file_name):
        return MemoryBackend.archive[self.folder_name][file_name]
