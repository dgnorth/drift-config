# -*- coding: utf-8 -*-
'''
    A Flask extension integrating relib.
'''
from __future__ import absolute_import

import logging

from flask import _app_ctx_stack, current_app
from flask import _app_ctx_stack as stack
from driftconfig.relib import get_store_from_url, create_backend



log = logging.getLogger(__name__)


class FlaskRelib(object):

    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.extensions['relib'] = self
        app.before_request(self._before_request)

    def refresh(self):
        """Invalidate Redis cache, if in use, and fetch new config from source."""
        ctx = stack.top
        if ctx is not None and hasattr(ctx, 'table_store'):
            delattr(ctx, 'table_store')

    def save(self, table_store):
        """Save 'table_store' to source."""
        b = create_backend(current_app.config.get('RELIB_CONFIG_URL'))
        table_store.save(b)
        self.refresh()

    @property
    def table_store(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'table_store'):
                url = current_app.config.get('RELIB_CONFIG_URL')
                ctx.table_store = get_store_from_url(url)
            return ctx.table_store

    def _before_request(self):
        '''
        Bla
        '''
        pass
        #print "GHOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOORK YESYESYEYSEYSE"
        #ctx = _app_ctx_stack.top
        ##if ctx is not None:
            #ctx.table_store = self._table_store
