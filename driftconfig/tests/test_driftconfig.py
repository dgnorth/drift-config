# -*- coding: utf-8 -*-
import unittest

from driftconfig.config import get_drift_table_store, TSTransaction

# TODO:
# - test 'check_only' in Table.add().


def create_basic_domain():
    ts = get_drift_table_store()
    ts.get_table('domain').add({
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

    def test_users(self):

        ts = create_basic_domain()

        user = ts.get_table('users').add({
            'organization_name': 'directivegames',
            'user_name': 'test_user',
        })

        # Create service user with a secret access key
        ts.get_table('users').add({
            'organization_name': 'directivegames',
            'user_name': 'ue4server',
            'is_service': True,
        })

        ts.get_table('users').add({
            'organization_name': 'directivegames',
            'user_name': 'alice',
            'is_role_admin': True,
            'access_key': '123',
            'secret_key': 'x',
            'password': 'x'
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

        role_service = ts.get_table('access-roles').add({
            'role_name': 'service',
            'deployable_name': 'drift-base',
            'description': "Full access to all API's",
        })

        role_client = ts.get_table('access-roles').add({
            'role_name': 'client',
            'deployable_name': 'drift-base',
            'description': "Full access to all API's",
        })

        ts.get_table('users-acl').add({
            'organization_name': 'directivegames',
            'user_name': user['user_name'],
            'role_name': role_service['role_name'],
        })

        ts.get_table('users-acl').add({
            'organization_name': 'directivegames',
            'user_name': user['user_name'],
            'role_name': role_client['role_name'],
            'tenant_name': 'dg-unittest-product',
        })


class TestPushPull(unittest.TestCase):

    @unittest.skip('')
    def test_ts_transaction(self):

        with TSTransaction() as ts:
            row = ts.get_table('domain').get()
            row['display_name'] += " moar! "


if __name__ == '__main__':
    unittest.main()
