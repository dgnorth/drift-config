# -*- coding: utf-8 -*-
'''
ReLib Backends
'''
import logging
import os
import StringIO

from .relib import Backend, BackendError

log = logging.getLogger(__name__)


class S3Backend(Backend):
    """
    S3 backend for TableStore.
    """
    def __init__(self, bucket_name, folder_name, region_name=None):
        import boto3
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.bucket_name = bucket_name
        self.folder_name = folder_name

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


class RedisBackend(Backend):

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


class FileBackend(Backend):

    def __init__(self, folder_name):
        folder_name = folder_name.replace('~', os.path.expanduser("~"))
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        self.folder_name = folder_name

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


class MemoryBackend(Backend):

    archive = {}

    def __init__(self, folder_name):
        self.folder_name = folder_name

    def __del__(self):
        del MemoryBackend.archive[self.folder_name]

    def __str__(self):
        return "MemoryBackend'{}'".format(self.folder_name)

    def save_data(self, file_name, data):
        MemoryBackend.archive[self.folder_name].setdefault({})[file_name] = data

    def load_data(self, file_name):
        return MemoryBackend.archive[self.folder_name][file_name]
