# -*- coding: utf-8 -*-
import logging

import os.path

from driftconfig.relib import TableStore
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



logging.basicConfig(level='INFO')
config_path = os.path.join(os.path.expanduser("~"), '.drift', 'config')
print "config_path is", config_path

# Set up backends. One on local hard drive, one on S3 and one in Redis
local_store = FileBackend(config_path)
s3_store = S3Backend('relib-test', 'drift-config', 'eu-west-1')
redis_store = RedisBackend()

# Create an empty config
ts = get_drift_table_store()

if 0:
    s3_store.load(ts)
    print "whee got all the config", ts
    redis_store.save(ts)
    print "now i have dumped all the s3 config into redis"
    local_store.save(ts)
    print "its also on mny local disk hes"
    config_path = os.path.join(os.path.expanduser("~"), '.drift', 'config2')
    FileBackend(config_path).save(ts)
    import sys
    sys.exit(1)

# Load from S3
#s3_store.load(ts)
#s3_store.save(ts)

# Chuck in some data
ts.get_table('organizations').add({'organization_name': 'directivegames', 'display_name': 'Directive Games', })
ts.get_table('tiers').add({'tier_name': 'LIVENORTH', 'organization_name': 'directivegames', 'is_live': True})
ts.get_table('tiers').add({'tier_name': 'DEVNORTH', 'organization_name': 'directivegames', 'is_live': False})
ts.get_table('tiers').add({'tier_name': 'DEVEAST', 'organization_name': 'directivegames', 'is_live': False})

ts.get_table('products').add({'product_name': 'superkaiju', 'organization_name': 'directivegames'})

ts.get_table('deployables').add({'organization_name': 'directivegames', 'deployable_name': 'drift-base', 'display_name': 'Drift Core Services', 'is_active': True, })
ts.get_table('deployables').add({'organization_name': 'directivegames', 'deployable_name': 'vignettes', 'display_name': 'Vignettes', })
ts.get_table('deployables').add({'organization_name': 'directivegames', 'deployable_name': 'game-server', 'display_name': 'Game Server Management', })
ts.get_table('deployables').add({'organization_name': 'directivegames', 'deployable_name': 'themachines-backend', 'display_name': 'The Machines Services', 'is_active': True, })
ts.get_table('deployables').add({'organization_name': 'directivegames', 'deployable_name': 'themachines-admin', 'display_name': 'The Machines Admin Web', })
ts.get_table('deployables').add({'organization_name': 'directivegames', 'deployable_name': 'kaleo-web', 'display_name': 'Kaleo Web', })
ts.get_table('deployables').add({'organization_name': 'directivegames', 'deployable_name': 'kards-backend', 'display_name': 'Kards Services', })
ts.get_table('deployables').add({'organization_name': 'directivegames', 'deployable_name': 'kaleometrics', 'display_name': 'Kaleo Metrics', })

# Configure LIVENORTH
for tenant_name in [
    'superkaiju', 'superkaiju-test', 'loadout', 'loadout-test', 'default-livenorth',
    'themachines', 'themachines-test', 'themacines-test2', 'nonnib-livenorth',
]:
    ts.get_table('tenant_names').add({'tenant_name': tenant_name})
    ts.get_table('tenants').add({'tier_name': 'LIVENORTH', 'deployable_name': 'drift-base', 'tenant_name': tenant_name,})
    ts.get_table('tenants').add({'tier_name': 'LIVENORTH', 'deployable_name': 'themachines-backend', 'tenant_name': tenant_name,})
    ts.get_table('tenants').add({'tier_name': 'DEVNORTH', 'deployable_name': 'drift-base', 'tenant_name': tenant_name,})

# Store locally and cache in Redis
ts.save_to_backend(local_store)

#ts.save_to_backend(s3_store)
#redis_store.save(ts)

ts = TableStore(local_store)
print "whee got ts", ts

