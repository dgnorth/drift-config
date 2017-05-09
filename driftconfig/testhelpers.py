# -*- coding: utf-8 -*-
from string import maketrans

from driftconfig.config import get_drift_table_store
from driftconfig.util import set_sticky_config


DOMAIN_NAME = 'testdomain'
ORG_NAME = 'acme'
TIER_NAME = 'TIER'
DEPL_NAME = 'svc'
PROD_NAME = 'prod'
TENANT_NAME = 'tenant'

# If serial numbers need to be replaced with letters
tr_upper = maketrans('0123456789', 'ABCDEFGHIJ')
tr_lower = maketrans('0123456789', 'abcdefghij')



def _add(fn, ts, name, config_size, count, **kw):
    if count == 1:
        fn(ts, name, config_size, **kw)
    else:
        for i in xrange(count):
            fn(ts, '{}{}'.format(name, i), config_size, **kw)


private_test_key = '''
-----BEGIN RSA PRIVATE KEY-----
MIIBygIBAAJhAOOEkKLzpVY5zNbn2zZlz/JlRe383fdnsuy2mOThXpJc9Tq+GuI+
PJJXsNa5wuPBy32r46/N8voe/zUG4qYrrRCRyjmV0yu4kZeNPSdO4uM4K98P1obr
UaYrik9cpwnu8QIDAQABAmA+BSAMW5CBfcYZ+yAlpwFVmUfDxT+YtpruriPlmI3Y
JiDvP21CqSaH2gGptv+qaGQVq8E1xcxv9jT1qK3b7wm7+xoxTYyU0XqZC3K+lGeW
5L+77H59RwQznG21FvjtRgECMQDzihOiirv8LI2S7yg11/DjC4c4lIzupjnhX2ZH
weaLJcjGogS/labJO3b2Q8RUimECMQDvKKKl1KiAPNvuylcrDw6+yXOBDw+qcwiP
rKysATJ2iCsOgnLC//Rk3+SN3R2+TpECMGjAglOOsu7zxu1levk16cHu6nm2w6u+
yfSbkSXaTCyb0vFFLR+u4e96aV/hpCfs4QIwd/I0aOFYRUDAuWmoAEOEDLHyiSbp
n34kLBLZY0cSbRpsJdHNBvniM/mKoo/ki/7RAjEAtpt6ixFoEP3w/2VLh5cut61x
E74vGa3+G/KdGO94ZnI9uxySb/czhnhvOGkpd9/p
-----END RSA PRIVATE KEY-----
    '''

public_test_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAYQDjhJCi86VWOc" \
    "zW59s2Zc/yZUXt/N33Z7Lstpjk4V6SXPU6vhriPjySV7DWucLjwct9q+Ovz" \
    "fL6Hv81BuKmK60Qkco5ldMruJGXjT0nTuLjOCvfD9aG61GmK4pPXKcJ7vE=" \
    " unittest@dg-api.com"


def create_test_domain(config_size=None):
    """
    Creates Drift config to use for testing.
    If 'deployable_name' is used, a simple set of single entry
    for all tables is created using that name, else some auto-
    generated names are used for all primary keys.
    'config_size' contains how many entries to create for each
    tables. The primary keys will have a running serial number
    added at the end.
    """
    config_size = config_size or {}

    config_size = {
        'num_org': config_size.get('num_org', 1),
        'num_tiers': config_size.get('num_tiers', 1),
        'num_deployables': config_size.get('num_deployables', 1),
        'num_products': config_size.get('num_products', 1),
        'num_tenants': config_size.get('num_tenants', 1),
    }

    ts = get_drift_table_store()
    ts.get_table('domain').add({
        'domain_name': DOMAIN_NAME,
        'display_name': "Unit Test Domain",
        'origin': ''
    })

    # First add all the tiers
    # Next, add all the deployables and associate with every tier
    # Then, "customes" can be added, organizations, products and tenants.
    _add(add_tier, ts, TIER_NAME, config_size, config_size['num_tiers'])
    _add(add_deployable, ts, DEPL_NAME, config_size, config_size['num_deployables'])
    _add(add_organization, ts, ORG_NAME, config_size, config_size['num_org'])

    set_sticky_config(ts)

    return ts


def add_tier(ts, tier_name, config_size):

    # Can't have numbers in tier names
    tier_name = tier_name.translate(tr_upper)

    ts.get_table('tiers').add({
        'tier_name': tier_name,
        'is_live': True,
        'aws': {
            'region': 'test-region-1',
            'ssh_key': 'unittest-key',
        },
    })


def add_deployable(ts, deployable_name, config_size):

    # Can't have numbers in deployable names
    deployable_name = deployable_name.translate(tr_lower)

    ts.get_table('deployable-names').add({
        'deployable_name': deployable_name,
        'display_name': "A service for {}".format(deployable_name),
    })

    # Associate deployable with all tiers
    for tier in ts.get_table('tiers').find():
        ts.get_table('deployables').add({
            'tier_name': tier['tier_name'],
            'deployable_name': deployable_name,
            'is_active': True,
            "jwt_trusted_issuers": [
                {
                    "iss": deployable_name,
                    "pub_rsa": public_test_key
                }
            ]
        })

        ts.get_table('public-keys').add({
            'tier_name': tier['tier_name'],
            'deployable_name': deployable_name,
            'keys': [
                {
                    'pub_rsa': public_test_key,
                    'private_key': private_test_key,
                }
            ]
        })


def add_organization(ts, organization_name, config_size):
    ts.get_table('organizations').add({
        'organization_name': organization_name,
        'short_name': organization_name,
        'display_name': 'Some Test Organization',
    })

    # Each organization has x many products
    _add(
        add_product, ts, PROD_NAME, config_size, config_size['num_products'],
        organization_name=organization_name
    )


def add_product(ts, product_name, config_size, organization_name):

    product_name = '{}-{}'.format(organization_name, product_name)

    ts.get_table('products').add({
        'product_name': product_name,
        'organization_name': organization_name,
    })

    # Each product has x many tenants
    _add(
        add_tenant, ts, TENANT_NAME, config_size, config_size['num_tenants'],
        product_name=product_name, organization_name=organization_name
    )

    ts.get_table('platforms').add({
        'product_name': product_name,
        'provider_name': 'test1',
        "provider_details": {
            "access_token": "four",
            "sekrit": "five"
        }})

    ts.get_table('platforms').add({
        'product_name': product_name,
        'provider_name': 'test2',
        "provider_details": {
            "appid": 123,
            "key": "fiftyfour"
        }})


def add_tenant(ts, tenant_name, config_size, product_name, organization_name):

    tenant_name = '{}-{}'.format(product_name, tenant_name)

    # Define a tenant
    ts.get_table('tenant-names').add({
        'tenant_name': tenant_name,
        'product_name': product_name,
        'organization_name': organization_name,
    })

    # Associate tenant on all tiers with all deployables
    for deployable in ts.get_table('deployables').find():
        ts.get_table('tenants').add({
            'tier_name': deployable['tier_name'],
            'deployable_name': deployable['deployable_name'],
            'tenant_name': tenant_name,
            'state': 'active',
        })

