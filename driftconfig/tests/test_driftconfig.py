# -*- coding: utf-8 -*-
import unittest
import string

from driftconfig.config import get_drift_table_store, TSTransaction
from ..relib import ConstraintError

# TODO:
# - test 'check_only' in Table.add().

DOMAIN = "unit_test_domain"
ORGANIZATION = "directivegames"
TIER = "UNITTEST"
DEPLOYABLE = "drift-base"
PRODUCT = "dg-unittest-product"
TENANT = "dg-unittest-product"

def create_basic_domain():
    ts = get_drift_table_store()
    ts.get_table('domain').add({
        'domain_name': DOMAIN,
        'display_name': string.capwords(DOMAIN.replace("_", " ")),
        'origin': ''
    })

    ts.get_table('organizations').add({
        'organization_name': ORGANIZATION,
        'short_name': 'dg',
        'display_name': 'Directive Games',
        })

    ts.get_table('tiers').add({
        'tier_name': TIER,
        'is_live': True,
        })

    ts.get_table('deployable-names').add({
        'deployable_name': DEPLOYABLE,
        'display_name': "Drift Base Services",
        })

    ts.get_table('deployables').add({
        'tier_name': TIER,
        'deployable_name': DEPLOYABLE,
        'is_active': True,
        })

    ts.get_table('products').add({
        'product_name': PRODUCT,
        'organization_name': 'directivegames',
        })

    ts.get_table('tenant-names').add({
        'tenant_name': TENANT,
        'product_name': PRODUCT,
        'tier_name': TIER,
        'organization_name': ORGANIZATION,
        })

    ts.get_table('tenants').add({
        'tier_name': TIER,
        'deployable_name': DEPLOYABLE,
        'tenant_name': TENANT,
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
            'key_type': 'custom',
            'custom_data': 'mr.bub',
            })

        ts.get_table('api-key-rules').add({
            'product_name': 'dg-unittest-product',
            'rule_name': 'dg-unittest-product-rule-one',
            'assignment_order': 1,
            'version_patterns': ['1.0.5', '1.0.8'],
            'is_active': True,
            'rule_type': 'pass',
            'response_header': {'message-from-mum': "Put your sweater on!"},
            })

    def test_ue4_gameservers_rules(self):
        """
        HACK! This should not be in drift-config repository at all as it's
        technically a 3rd party stuff.
        """
        ts = create_basic_domain()

        ts.get_table('ue4-gameservers-config').add({
            'build_archive_defaults': {
                'region': 'eu-west-1',
                'bucket_name': 'directive-tiers.dg-api.com',
                'ue4_builds_folder': 'ue4-builds',
            }
            })

        ts.get_table('ue4-build-artifacts').add({
            'product_name': 'dg-unittest-product',
            's3_region': 'eu-west-1',
            'bucket_name': 'directive-tiers.dg-api.com',
            'path': 'ue4-builds/directivegames/SuperKaijuVR',
            'command_line': '',
            })

        ts.get_table('gameservers-machines').add({
            'product_name': 'dg-unittest-product',
            'group_name': 'main',
            'region': 'eu-west-1',
            'platform': 'windows',
            'autoscaling': {
                'min': 1,
                'max': 4,
                'desired': 2,
                'instance_type': 'm2.medium',
            },
            })

        ts.get_table('gameservers-instances').add({
            'gameserver_instance_id': 5000,
            'product_name': 'dg-unittest-product',
            'group_name': 'main',
            'region': 'eu-west-1',
            'tenant_name': 'dg-unittest-product',
            'ref': '1.6.5',
            'processes_per_machine': 4,
            'command_line': '-bingo',
            })

        row = ts.get_table('gameservers-instances').add({
            'product_name': 'dg-unittest-product',
            'group_name': 'main',
            'region': 'eu-west-1',
            'tenant_name': 'dg-unittest-product',
            'ref': '1.6.5',
            'processes_per_machine': 4,
            'command_line': '-gr0ndal',
            })
        self.assertEqual(row['gameserver_instance_id'], 5001)

    def test_service_users(self):
        ts = create_basic_domain()
        user_name = 'test_user'
        with self.assertRaises(ConstraintError):
            ts.get_table('users').add({
                'user_name': user_name,
                'tenant_name': 'bleh'
            })
        ts.get_table('users').add({
            'user_name': user_name,
            'tenant_name': TENANT
        })

        # Create service user with a secret access key
        with self.assertRaises(ConstraintError):
            ts.get_table('access-keys').add({
                'user_name': user_name + 'junk',
                'tenant_name': TENANT,
                'access_key': "53cretKey"
            })
        with self.assertRaises(ConstraintError):
            ts.get_table('access-keys').add({
                'user_name': user_name,
                'tenant_name': TENANT+ 'junk',
                'access_key': "53cretKey"
            })
        ts.get_table('access-keys').add({
            'user_name': user_name,
            'tenant_name': TENANT,
            'access_key': "53cretKey"
        })

        # Create service credentials with client_id and client_secret
        with self.assertRaises(ConstraintError):
            ts.get_table('client-credentials').add({
                'user_name': user_name+'junk',
                'tenant_name': TENANT,
                'client_id': '1a2b3c4d5e6f',
                'client_secret': '8ig5ecret'
            })
        with self.assertRaises(ConstraintError):
            ts.get_table('client-credentials').add({
                'user_name': user_name,
                'tenant_name': TENANT+'junk',
                'client_id': '1a2b3c4d5e6f',
                'client_secret': '8ig5ecret'
            })
        ts.get_table('client-credentials').add({
            'user_name': user_name,
            'tenant_name': TENANT,
            'client_id': '1a2b3c4d5e6f',
            'client_secret': '8ig5ecret'
        })

        # print "USER", json.dumps(service_user, indent=4)

        """
        local developer running ue4 server locally
        POST /auth
        data = {
            "provider": "access_key",
            "access_key": "95Fqeb5qXrnS4jD2M9c46RXAr8IF1gJy",
            "secret_key": "pMrZoJaiqNRpaza9TaBFOBsGnN4sFZNY"
        }

        AWS ec2 instance
        POST /auth
        data = {
            "provider": "aws_ec2_role",
            "username": "ue4server",
            "aws_signature": "q2WlGeJUxEbO"
        }

        Interactive web login
        POST /auth
        data = {
            "provider": "username_and_password",
            "username": "alice",
            "password": "asdf12345"
        }

        -> identity: "drift:dg.ue4server"
        -> identity: "drift:dg.alice"
        """


class TestPushPull(unittest.TestCase):

    @unittest.skip('')
    def test_ts_transaction(self):

        with TSTransaction() as ts:
            row = ts.get_table('domain').get()
            row['display_name'] += " moar! "


if __name__ == '__main__':
    unittest.main()
