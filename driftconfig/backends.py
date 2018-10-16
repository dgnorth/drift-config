# -*- coding: utf-8 -*-
'''
ReLib Backends
'''
import logging
import os
import six
from six.moves import cStringIO as StringIO
from six.moves.urllib.parse import urlparse
import zipfile

from .relib import Backend, BackendError, BackendFileNotFound, register

log = logging.getLogger(__name__)


@register
class S3Backend(Backend):
    """
    S3 backend for TableStore.
    """

    __scheme__ = 's3'
    default_format = 'pickle'

    def __init__(self, bucket_name, folder_name, region_name=None, etag=None):
        import boto3
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.bucket_name = bucket_name
        self.folder_name = folder_name.lstrip('/')  # Strip leading slashes
        self.region_name = region_name
        self.etag = etag

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
        return "S3Backend'{}'".format(self.get_url())

    def get_key_name(self, file_name):
        return '{}/{}'.format(self.folder_name, file_name)

    def save_data(self, file_name, data):
        return self._save_data_with_bucket_logic(file_name, data, try_create_bucket=True)

    def _save_data_with_bucket_logic(self, file_name, data, try_create_bucket):
        from botocore.client import ClientError
        f = StringIO(data)
        key_name = self.get_key_name(file_name)
        log.debug("Uploading %s bytes to s3://%s/%s", len(data), self.bucket_name, key_name)
        try:
            self.s3_client.upload_fileobj(
                f,
                self.bucket_name,
                key_name,
                ExtraArgs={'ContentType': 'application/json'},
            )
        except ClientError as e:
            if 'NoSuchBucket' in str(e) and try_create_bucket:
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region_name}
                )
                return self._save_data_with_bucket_logic(file_name, data, try_create_bucket=False)
            else:
                raise

    def load_data(self, file_name):
        from botocore.client import ClientError
        key_name = self.get_key_name(file_name)
        log.debug("Downloading s3://%s/%s", self.bucket_name, key_name)
        f = six.BytesIO()
        try:
            self.s3_client.download_fileobj(self.bucket_name, key_name, f)
        except ClientError as e:
            if '404' in str(e):
                raise BackendFileNotFound
        return f.getvalue()


@register
class RedisBackend(Backend):

    __scheme__ = 'redis'
    default_format = 'pickle'

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
            socket_timeout=5.0,
        )

        self.host, self.port, self.db = host, port, db
        log.debug("%s initialized.", self)

    @classmethod
    def create_from_url_parts(cls, parts, query):
        db = int(query['db'][0]) if 'db' in query else None
        prefix = query['prefix'][0] if 'prefix' in query else ''
        expire_sec = int(query['expire_sec'][0]) if 'expire_sec' in query else None
        b = cls(host=parts.hostname, port=parts.port, db=db, prefix=prefix, expire_sec=expire_sec)
        return b  # ZipEncoded(b)

    @classmethod
    def create_from_server_info(cls, host, port, domain_name):
        b = cls(
            host=host,
            port=port,
            prefix=domain_name,
            expire_sec=None,  # Never expires
            )
        return b  # ZipEncoded(b)

    def __str__(self):
        return "RedisBackend'{}:{}#{}, prefix={}'".format(self.host, self.port, self.db, self.prefix)

    def get_key_name(self, file_name):
        return 'relib:drift-config:{}:{}'.format(self.prefix, file_name)

    def save_data(self, file_name, data):
        key_name = self.get_key_name(file_name)
        log.debug("Adding %s bytes to Redis:%s with expiry:%s", len(data), key_name, self.expire_sec)
        self.conn.set(key_name, data)
        if self.expire_sec is not None:
            self.conn.expire(key_name, self.expire_sec)
        self.conn.set

    def load_data(self, file_name):
        key_name = self.get_key_name(file_name)
        log.debug("Reading from Redis:%s", key_name)
        data = self.conn.get(key_name)
        if data is None:
            log.warning("Redis cache doesn't have '{}'. (Is it expired?)".format(key_name))
            raise BackendFileNotFound
        return data


    def get_url(self):
        return "redis://{}:{}/{}?prefix={}".format(self.host, self.port, self.db, self.prefix)


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
            log.debug("Writing %s bytes to %s", len(data), path_name)
            f.write(data)

    def load_data(self, file_name):
        path_name = self.get_filename(file_name)
        log.debug("Reading from %s", path_name)

        import os.path
        if not os.path.exists(path_name):
            raise BackendFileNotFound

        with open(path_name, 'r') as f:
            return f.read()


@register
class MemoryBackend(Backend):

    __scheme__ = 'memory'
    archive = {}

    def __init__(self, folder_name):
        self.folder_name = folder_name
        MemoryBackend.archive[folder_name] = {}

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
        MemoryBackend.archive[self.folder_name][file_name] = data

    def load_data(self, file_name):
        if file_name not in MemoryBackend.archive[self.folder_name]:
            raise BackendFileNotFound
        return MemoryBackend.archive[self.folder_name][file_name]


class ZipEncoded(Backend):
    """Aggregate class which serializes to and from a single zip file."""
    def __init__(self, aggregate):
        self.aggregate = aggregate

    def start_saving(self):
        self._fp = StringIO()
        self._zipfile = zipfile.ZipFile(self._fp, mode='w', compression=zipfile.ZIP_DEFLATED)

    def done_saving(self):
        self.aggregate.save_data("_zipped.zip", self._fp.getvalue())

    def start_loading(self):
        self._fp = StringIO()
        self._fp.write(self.aggregate.load_data("_zipped.zip"))
        self._fp.seek(0)
        self._zipfile = zipfile.ZipFile(self._fp)

    def done_loading(self):
        pass

    def save_data(self, file_name, data):
        self._zipfile.writestr(file_name, data)

    def load_data(self, file_name):
        self._zipfile.read(file_name)
