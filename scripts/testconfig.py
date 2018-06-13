# -*- coding: utf-8 -*-
import logging
from datetime import datetime

import os.path
from click import echo

from driftconfig.relib import TableStore, create_backend
from driftconfig.backends import FileBackend, S3Backend, RedisBackend
from driftconfig.config import get_drift_table_store

# tenant name:
"<org name>-<tier name>-<product name>"
'''
directivegames-superkaiju-LIVENORTH.dg-api.com
directivegames-superkaiju-DEVNORTH.dg-api.com

superkaiju.dg-api.com
directivegames-borkbork

.dg-api.com


'''

def main():


    logging.basicConfig(level='INFO')
    config_path = os.path.join(os.path.expanduser("~"), '.drift', 'config')
    echo("config_path is " + config_path)

    # Set up backends. One on local hard drive, one on S3 and one in Redis
    s3_store = S3Backend('relib-test', 'directive-games', 'eu-west-1')
    s3_store = create_backend('s3://relib-test/directive-games')
    redis_store = RedisBackend()

    # Create an empty config
    ts = get_drift_table_store()

    if 0:
        s3_store.load(ts)
        echo("whee got all the config " + ts)
        redis_store.save(ts)
        echo("now i have dumped all the s3 config into redis")
        local_store.save(ts)
        echo("its also on mny local disk hes")
        config_path = os.path.join(os.path.expanduser("~"), '.drift', 'config2')
        FileBackend(config_path).save(ts)
        import sys
        sys.exit(1)

    # Load from S3
    #s3_store.load(ts)
    #s3_store.save(ts)

    # Chuck in some data
    ts.get_table('domain').add({'domain_name': 'dgnorth', 'display_name': 'Directive Games North', 'origin': 's3://relib-test/directive-games-v2?region=eu-west-1'})
    ts.get_table('organizations').add({'organization_name': 'directivegames', 'display_name': 'Directive Games', })
    ts.get_table('tiers').add({'tier_name': 'LIVENORTH', 'organization_name': 'directivegames', 'is_live': True})
    ts.get_table('tiers').add({'tier_name': 'DEVNORTH', 'organization_name': 'directivegames', 'is_live': False})
    ts.get_table('tiers').add({'tier_name': 'DEVEAST', 'organization_name': 'directivegames', 'is_live': False})

    ts.get_table('products').add({'product_name': 'superkaiju', 'organization_name': 'directivegames'})

    ts.get_table('deployable_names').add({'tier_name': 'LIVENORTH', 'deployable_name': 'drift-base', 'display_name': 'Drift Core Services', })
    ts.get_table('deployable_names').add({'tier_name': 'LIVENORTH', 'deployable_name': 'vignettes', 'display_name': 'Vignettes', })
    ts.get_table('deployable_names').add({'tier_name': 'LIVENORTH', 'deployable_name': 'game-server', 'display_name': 'Game Server Management', })
    ts.get_table('deployable_names').add({'tier_name': 'LIVENORTH', 'deployable_name': 'themachines-backend', 'display_name': 'The Machines Services', })
    ts.get_table('deployable_names').add({'tier_name': 'LIVENORTH', 'deployable_name': 'themachines-admin', 'display_name': 'The Machines Admin Web', })
    ts.get_table('deployable_names').add({'tier_name': 'LIVENORTH', 'deployable_name': 'kaleo-web', 'display_name': 'Kaleo Web', })
    ts.get_table('deployable_names').add({'tier_name': 'LIVENORTH', 'deployable_name': 'kards-backend', 'display_name': 'Kards Services', })
    ts.get_table('deployable_names').add({'tier_name': 'LIVENORTH', 'deployable_name': 'kaleometrics', 'display_name': 'Kaleo Metrics', })

    ts.get_table('deployables').add({'tier_name': 'LIVENORTH', 'deployable_name': 'drift-base', 'is_active': True, })
    ts.get_table('deployables').add({'tier_name': 'LIVENORTH', 'deployable_name': 'themachines-backend', 'is_active': True, })


    # Configure LIVENORTH
    for tenant_name in [
        'superkaiju', 'superkaiju-test', 'loadout', 'loadout-test', 'default-livenorth',
        'themachines', 'themachines-test', 'themacines-test2', 'nonnib-livenorth',
    ]:
        ts.get_table('tenant-names').add({
            'tenant_name': tenant_name,
            'organization_name': 'directivegames', 'product_name': 'superkaiju',
            'reserved_at': datetime.utcnow().isoformat(),
            'reserved_by': 'prezidentbongo',

        })
        ts.get_table('tenants').add({'tier_name': 'LIVENORTH', 'deployable_name': 'drift-base', 'tenant_name': tenant_name,})
        ts.get_table('tenants').add({'tier_name': 'LIVENORTH', 'deployable_name': 'themachines-backend', 'tenant_name': tenant_name,})
        ts.get_table('tenants').add({'tier_name': 'DEVNORTH', 'deployable_name': 'drift-base', 'tenant_name': tenant_name,})

    # Store locally and cache in Redis
    domain_name = ts.get_table('domain')['domain_name']
    echo("DOMAIN NAME IS", domain_name)
    local_store = create_backend('file://./~/.drift/config/' + domain_name)
    echo("LOCAL STORE BACKEND IS %s %s" % (local_store, local_store.get_url()))


    local_store.save_table_store(ts)

    s3_store.save_table_store(ts)
    #redis_store.save(ts)

    ts = local_store.load_table_store()
    echo("whee got ts " + ts)

    '''
    TODO: unit test failed testing:
     - default values were overriding actual input, not vice versa. its fixed though.
     - remove() function not tested.
     - backend url functionality not tested.
     -
    '''

if __name__ == "__main__":
    main()