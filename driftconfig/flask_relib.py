# -*- coding: utf-8 -*-
'''
    A Flask extension integrating relib.
'''
from __future__ import absolute_import

import logging

from flask import _app_ctx_stack as stack
from driftconfig.relib import CHECK_INTEGRITY
from driftconfig.config import get_default_drift_config


log = logging.getLogger(__name__)


class FlaskRelib(object):

    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.extensions['relib'] = self
        if not app.debug:
            del CHECK_INTEGRITY[:]

    def refresh(self):
        """Invalidate Redis cache, if in use, and fetch new config from source."""
        ctx = stack.top
        if ctx is not None and hasattr(ctx, 'table_store'):
            delattr(ctx, 'table_store')

    @property
    def table_store(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'table_store'):
                ctx.table_store = self._get_table_store()
            return ctx.table_store

    def _get_table_store(self):
        return get_default_drift_config()

