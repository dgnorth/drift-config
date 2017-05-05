# -*- coding: utf-8 -*-
from driftconfig.config import get_drift_table_store
from driftconfig.util import set_sticky_config

def create_test_domain(deployable_name):
    product_name = 'dg-unittest-product'
    tier_name = 'UNITTEST'

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
        'tier_name': tier_name,
        'is_live': True,
        'aws': {
            'region': 'test-region-1',
            'ssh_key': 'unittest-key',
        },
        })

    ts.get_table('deployable-names').add({
        'deployable_name': deployable_name,
        'display_name': "Drift Base Services",
        })

    ts.get_table('deployables').add({
        'tier_name': tier_name,
        'deployable_name': deployable_name,
        'is_active': True,
        "jwt_trusted_issuers": [
            {
                "iss": deployable_name,
                "pub_rsa": public_test_key
            }
        ]
        })

    ts.get_table('products').add({
        'product_name': product_name,
        'organization_name': 'directivegames',
        })

    ts.get_table('tenant-names').add({
        'tenant_name': 'dg-unittest-product',
        'product_name': 'dg-unittest-product',
        'tier_name': tier_name,
        'organization_name': 'directivegames',
        })

    ts.get_table('tenants').add({
        'tier_name': tier_name,
        'deployable_name': deployable_name,
        'tenant_name': 'dg-unittest-product',
        'state': 'active',
        })

    ts.get_table('public-keys').add({
        'tier_name': tier_name,
        'deployable_name': deployable_name,
        'keys': [
            {
                'pub_rsa': public_test_key,
                'private_key': private_test_key,
            }
        ]
        })

    ts.get_table('platforms').add({
        'product_name': product_name,
        'provider_name': 'oculus',
        "provider_details": {
            "access_token": "four",
            "sekrit": "five"
        }})

    ts.get_table('platforms').add({
        'product_name': product_name,
        'provider_name': 'steam',
        "provider_details": {
            "appid": 123,
            "key": "fiftyfour"
        }})

    set_sticky_config(ts)
    return ts

