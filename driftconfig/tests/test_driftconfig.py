# -*- coding: utf-8 -*-
import unittest
import json
import tempfile
import shutil

import jsonschema

from driftconfig.relib import TableStore, Table, TableError, ConstraintError, Backend
from driftconfig.backends import FileBackend

from driftconfig.config import get_drift_table_store

# TODO:
# - test 'check_only' in Table.add().


class TestBackend(Backend):
    """Wrap a dict as a Backend for TableStore."""
    def __init__(self, storage):
        self.storage = storage

    def save_data(self, k, v):
        self.storage[k] = v

    def load_data(self, k):
        return self.storage[k]

def create_basic_domain():
    ts = get_drift_table_store()
    domain = ts.get_table('domain').add({
        'domain_name': 'unit_test_domain',
        'display_name': "Unit Test Domain",
        'origin': ''
    })

    ts.get_table('organizations').add({
        'organization_name': 'directivegames',
        'short_name': 'dg',
        'display_name': 'Directive Games',
        })

    ts.get_table('tiers').add({
        'tier_name': 'UNITTEST',
        'is_live': True,
        })

    ts.get_table('deployable-names').add({
        'deployable_name': 'drift-base',
        'display_name': "Drift Base Services",
        })

    ts.get_table('deployables').add({
        'tier_name': 'UNITTEST',
        'deployable_name': 'drift-base',
        'is_active': True,
        })

    ts.get_table('products').add({
        'product_name': 'dg-unittest-product',
        'organization_name': 'directivegames',
        })

    ts.get_table('tenant-names').add({
        'tenant_name': 'dg-unittest-product',
        'product_name': 'dg-unittest-product',
        'tier_name': 'UNITTEST',
        'organization_name': 'directivegames',
        })

    ts.get_table('tenants').add({
        'tier_name': 'UNITTEST',
        'deployable_name': 'drift-base',
        'tenant_name': 'dg-unittest-product',
        'state': 'active',
        })

    return ts



class TestRelib(unittest.TestCase):

    def setUp(self):
        pass

    def test_api_router_rules(self):
        """
        HACK! This should not be in drift-config repository at all as it's
        technically a 3rd party stuff.
        """
        ts = create_basic_domain()

        ts.get_table('api-keys').add({
            'api_key_name': 'matti-555',
            'product_name': 'dg-unittest-product',
            'user_name': 'matti',
            })

        ts.get_table('api-key-rules').add({
            'rule_name': 'dg-unittest-product-rule-one',
            'product_name': 'dg-unittest-product',
            'rule_type': 'pass',
            'response_header': {'message-from-mum': "Put your sweater on!"},
            })

        ts.get_table('api-rule-assignments').add({
            'api_key_name': 'matti-555',
            'match_type': 'exact',
            'assignment_order': 1,
            'version_patterns': ['1.0.5', '1.0.8'],
            'rule_name': 'dg-unittest-product-rule-one',
            })









if __name__ == '__main__':
    unittest.main()

