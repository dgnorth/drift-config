# -*- coding: utf-8 -*-
'''
    A Flask extension integrating relib.
'''
from __future__ import absolute_import

import logging

from flask import current_app
from flask import _app_ctx_stack as stack
from driftconfig.relib import get_store_from_url, create_backend, copy_table_store
from driftconfig.util import get_domains


log = logging.getLogger(__name__)


class FlaskRelib(object):

    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.extensions['relib'] = self

    def refresh(self):
        """Invalidate Redis cache, if in use, and fetch new config from source."""
        ctx = stack.top
        if ctx is not None and hasattr(ctx, 'table_store'):
            delattr(ctx, 'table_store')

    def save(self, table_store=None):
        """Save 'table_store' to source. If 'table_store' is not specified, the default one is used."""
        table_store = table_store or self.table_store
        origin = table_store.get_table('domain')['origin']
        origin_backend = create_backend(origin)
        table_store.save_to_backend(origin_backend)
        self.refresh()

    @property
    def table_store(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'table_store'):
                ctx.table_store = self._get_table_store()
            return ctx.table_store

    def _get_table_store(self):
        if 'DRIFT_CONFIG_URL' not in current_app.config:
            domains = get_domains()
            if len(domains) != 1:
                raise RuntimeError("'DRIFT_CONFIG_URL' not in app.config and no single "
                                   "candidate found in ~/.drift/config")
            domain = domains.values()[0]
            return domain['table_store']
        else:
            url = current_app['DRIFT_CONFIG_URL']
            return get_store_from_url(url)

    def get_copy(self):
        """"Return a copy of current table store. Good for editing."""
        return copy_table_store(self.table_store)
