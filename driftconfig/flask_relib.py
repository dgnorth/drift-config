# -*- coding: utf-8 -*-
'''
    A Flask extension integrating relib.
'''
from __future__ import absolute_import

import logging

from flask import _app_ctx_stack
from driftconfig.relib import get_store_from_url


log = logging.getLogger(__name__)


class FlaskRelib(object):

    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.extensions['relib'] = self
        app.before_request(self._before_request)
        self._table_store = None

        url = app.config.get('RELIB_CONFIG_URL')
        if url:
            try:
                self._table_store = get_store_from_url(url)
            except Exception:
                log.exception("Relib DB failed loading from '%s'.", url)

    def _before_request(self):
        '''
        Bla
        '''
        setattr(_app_ctx_stack.top, 'ts', self._table_store)
