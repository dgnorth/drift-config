# -*- coding: utf-8 -*-
'''
    A Flask extension integrating relib.
'''
from __future__ import absolute_import

import logging
import os

from flask import _app_ctx_stack, current_app, request, session
from driftconfig.config import ConfigSession
from driftconfig.relib import TableStore, BackendError
from driftconfig.backends import FileBackend, S3Backend, RedisBackend


log = logging.getLogger(__name__)


class FlaskRelib(object):

    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.extensions['relib'] = self
        app.before_request(self._before_request)

        self._config_root = app.config.get(
            'RELIB_CONFIG_ROOT',
            os.path.join(app.instance_path, 'config')
        )
        self._file_store = FileBackend(self._config_root)
        self._table_store = TableStore()

        try:
            self._table_store.load_from_backend(self._file_store)
        except Exception:
            log.exception("Relib DB failed loading from '%s'.", self._config_root)

    def _before_request(self):
        '''
        Bla
        '''
        setattr(_app_ctx_stack.top, 'ts', self._table_store)
