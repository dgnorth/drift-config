# -*- coding: utf-8 -*-
'''
Drift Config Utility and helpers functions

some important bits to do:

tier init eu-west/..../blah


concepts:

organization
tier
tenant
service/deployable



objects:

deployable + name,ssh_key = used by several commands (ami, quickdeploy, remotecmd, ssh, logs, tier)

deployable + name,api               = used by api-router configgenerator, kaleo web
deployable + autoscaling,release    = used by api-router configgenerator




drift-config core tables:


organizations:
    organization_name       string, unique


tiers:
    organization            fk->organizations
    tier_name               string, unique
    is_live                 bool


deployables:
    deployable_name         string, unique
    organization            fk->organizations


tenants:
    # a tenant name is unique across all tiers
    tenant_name             string, unique
    tier                    fk->tiers

    deployables
        deployable_name         fk->deployables


authentication:
    tier                    fk->tiers
    deployable_name         fk->deployables
    keys
        issued              datetime
        expires             datetime
        pub_rsa             string
        private_key         string      (note, this entry only accessible to the issuing service)

    authentications
        # 3rd party authentication
        tenant_name         fk->tenants
        provider_name       string
        provider_details    dict

    access_keys
        # replacing the global 'service_user'
        user_name           string
        roles               list of role names
        access_keys         string
        secret_key          string
        valid_until         datetime
        is_active           bool


application specific tables:

api_router:
    tier                    fk->tiers
    services
        deployable_name     fk->deployables
        api                 string, unique
        autoscaling         dict
        release             string

    api_keys
        api_key_name        string, unique
        api_key_id          string
        api_key_rules       dict


aws_tools:
    services
        deployable_name     fk->deployables
        ssh_key             string


drift_base:
    tier                    fk->tiers
    db_connection_info      dict (RDS stuff)


server_daemon:
    access_key              string
    secret_key              string




refactor "deployable":

drift-config defines:

    service/deployable:
        name:       name of deployable
        tenants:    array of tenant names.


api-router specifics:
        "deployables":
        {
            "name": "<deployable name>",
            "api": "<api shortname>",
            "autoscaling": {},
            "release": "..."
        },
        "api_keys":
        [
            {},...
        ]


aws commands extend service/deployable:

        "aws_tools":
        {
            "ssh_key": "keyname"
        }




'''
import logging


from driftconfig.relib import TableStore, BackendError
from driftconfig.backends import FileBackend, S3Backend, RedisBackend

log = logging.getLogger(__name__)


# tenant name:
"<org name>-<tier name>-<product name>"
'''
directivegames-superkaiju-LIVENORTH.dg-api.com
directivegames-superkaiju-DEVNORTH.dg-api.com

superkaiju.dg-api.com
directivegames-borkbork

.dg-api.com


'''


def get_drift_table_store():
    """
    Create a Data Store which contains all Core Drift tables.
    """

    # RULE: pk='tier_name'='LIVENORTH', role['liveops', 'admin', 'service']

    ts = TableStore()
    organizations = ts.add_table('organizations')
    organizations.add_primary_key('organization_name')
    organizations.add_schema({
        'type': 'object',
        'properties': {
            'display_name': {'type': 'string'},
        },
    })

    tiers = ts.add_table('tiers')
    tiers.add_primary_key('tier_name')
    tiers.add_foreign_key('organization_name', 'organizations')
    tiers.add_schema({
        'type': 'object',
        'properties': {
            'tier_name': {'pattern': r'^([A-Z]){3,20}$'},
            'is_live': {'type': 'boolean'},
        },
        'required': ['organization_name', 'is_live'],
    })
    tiers.add_default_values({'is_live': True})

    deployables = ts.add_table('deployables')
    deployables.add_primary_key('deployable_name')
    deployables.add_foreign_key('organization_name', 'organizations')
    deployables.add_schema({
        'type': 'object',
        'properties': {
            'display_name': {'type': 'string'},
            'is_active': {'type': 'boolean'},
        },
        'required': ['display_name', 'is_active'],
    })
    deployables.add_default_values({'is_active': False})

    tenant_names = ts.add_table('tenant_names')
    tenant_names.add_primary_key('tenant_name')

    tenants = ts.add_table('tenants')
    tenants.add_primary_key('tier_name,deployable_name,tenant_name')
    tenants.set_row_as_file(use_subfolder=True, group_by='tier_name,tenant_name')
    tenants.add_foreign_key('tier_name', 'tiers')
    tenants.add_foreign_key('deployable_name', 'deployables')
    tenants.add_foreign_key('tenant_name', 'tenant_names')
    tenants.add_schema({
        'type': 'object',
        'properties': {
            'description': {'type': 'string'},
            'state': {'type': {'enum': ['initializing', 'active', 'disabled', 'deleted']}},
        },
    })

    products = ts.add_table('products')
    products.add_primary_key('organization_name,product_name')
    products.add_foreign_key('organization_name', 'organizations')
    products.set_row_as_file(use_subfolder=True, group_by='product_name')

    public_keys = ts.add_table('public-keys')
    public_keys.set_row_as_file(use_subfolder=True, subfolder_name='authentication')
    public_keys.add_primary_key('tier_name,deployable_name')
    public_keys.add_foreign_key('tier_name', 'tiers')
    public_keys.add_foreign_key('deployable_name', 'deployables')
    public_keys.add_schema({
        'type': 'object',
        'properties': {
            'keys': {'type': 'array', 'items': {
                'type': 'object',
                'properties': {
                    'issued': {'format': 'date-time'},
                    'expires': {'format': 'date-time'},
                    'pub_rsa': {'type': 'string'},
                    'private_key': {'type': 'string'},
                },
            }},
        },
    })

    platforms = ts.add_table('platforms')
    platforms.set_row_as_file(use_subfolder=True, subfolder_name='authentication')
    platforms.add_primary_key('tier_name,deployable_name,tenant_name')
    platforms.add_foreign_key('tier_name', 'tiers')
    platforms.add_foreign_key('deployable_name', 'deployables')
    platforms.add_foreign_key('tier_name,deployable_name,tenant_name', 'tenants')
    platforms.add_schema({
        'type': 'object',
        'properties': {
            'providers': {'type': 'array', 'items': {
                'type': 'object',
                'properties': {
                    'provider_name': {'type': 'string'},
                    'provider_details': {'type': 'object'},
                },
            }},
        },
    })

    definition = ts.get_definition()
    new_ts = TableStore()
    new_ts.init_from_definition(definition)
    return new_ts


class ConfigSession(object):
    """
    This class wraps the Drift Config Database.
    It persists the data on S3 and uses Redis to cache and distribute state.

    Usage:

    This class can be created and thrown away at will as it's supposed to
    be short lived. It's also possible to call refresh() to fetch in the
    most recent data.

    Make sure to call save() to persist any changes to S3.

    """
    def __init__(self):
        self.redis_store = RedisBackend(expire_sec=53)
        self.s3_store = S3Backend('relib-test', 'kaleo-web-1', 'eu-west-1')
        self.file_store = FileBackend()
        self.refresh()

    def refresh(self):
        try:
            self.ts = TableStore(self.redis_store)
        except BackendError:
            # Fetch from S3
            log.info("ConfigSession: Cache miss, fetching from S3..")
            # TODO: Lock fetch
            self.ts = TableStore(self.s3_store)
            self.ts.save_to_backend(self.redis_store)


    def save(self):
        """
        Save the data in 'ts'.
        """
        self.ts.save_to_backend(self.s3_store)
        self.ts.save_to_backend(self.redis_store)

    def __getitem__(self, table_name):
        return self.ts.get_table(table_name)

    def tenant_exists(self, tenant_name):
        tenant = self['tenant_names'].get({'tenant_name': tenant_name})
        return tenant is not None

    def get_all_tenant_names(self):
        return [t['tenant_name'] for t in self['tenant_names'].find()]


CREATEDB = 0

if __name__ == '__main__':
    logging.basicConfig(level='INFO')

    c = ConfigSession()

    if CREATEDB:
        # Create DB
        c.ts = get_drift_table_store()
        c.save()

    c['tenant_names'].add({'tenant_name': 'bloorgh'})
    c['tenants'].add({
        'tier_name','deployable_name','tenant_name'
        })
    print "existsts", c.tenant_exists('boo')
    print "all tenants", c.get_all_tenant_names()
    #c.save_config()


