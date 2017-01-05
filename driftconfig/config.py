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


domain (single row):
    domain_name             string, required
    display_name            string
    origin                  string, required


organizations:
    organization_name       string, pk
    display_name: string


tiers:
    tier_name               string, pk
    is_live                 boolean, default=true


deployable_names:
    deployable_name         string, pk
    display_name            string, required
    description             string


deployables:
    tier_name               string, pk, fk->tiers
    deployable_name         string, pk, fk->deployables
    is_active               boolean, default=false


products:
    product_name            string, pk
    organization_name       string, pk, fk->organizations


tenant_names:
    # a tenant name is unique across all tiers
    tenant_name             string, pk
    tier_name               fk->tiers
    product_name            fk->products
    reserved_at             date-time
    reserved_by             date-time


tenants:
    tier_name               string, pk, fk->tiers
    deployable_name         string, pk, fk->deployables
    tenant_name             string, pk, fk->tenants
    state                   enum initializing|active|disabled|deleted, default=initializing

    meta
        use_subfolder=true
        group_by=tier_name,tenant_name


public-keys:
    tier_name               string, pk, fk->tiers
    deployable_name         string, pk, fk->deployables
    keys                    array

    meta
        subfolder_name=authentication


platforms:
    tier_name               string, pk, fk->tiers
    deployable_name         string, pk, fk->deployables
    tenant_name             string, pk, fk->tenants
    providers               array

    meta
        subfolder_name=authentication



******************************************************************

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
import os
import os.path

from driftconfig.relib import TableStore, BackendError, get_store_from_url
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

    domain = ts.add_table('domain', single_row=True)
    domain.add_schema({
        'type': 'object',
        'properties': {
            'domain_name': {'type': 'string'},
            'display_name': {'type': 'string'},
            'origin': {'type': 'string'},
        },
        'required': ['domain_name', 'origin'],
    })

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
    tiers.add_schema({
        'type': 'object',
        'properties': {
            'tier_name': {'pattern': r'^([A-Z]){3,20}$'},
            'is_live': {'type': 'boolean'},
        },
        'required': ['is_live'],
    })
    tiers.add_default_values({'is_live': True})

    deployable_names = ts.add_table('deployable_names')
    deployable_names.add_primary_key('deployable_name')
    deployable_names.add_schema({
        'type': 'object',
        'properties': {
            'deployable_name': {'pattern': r'^([a-z-]){3,20}$'},
            'display_name': {'type': 'string'},
            'description': {'type': 'string'},
        },
        'required': ['display_name'],
    })

    deployables = ts.add_table('deployables')
    deployables.add_primary_key('tier_name,deployable_name')
    deployables.add_foreign_key('tier_name', 'tiers')
    deployables.add_foreign_key('deployable_name', 'deployable_names')
    deployables.add_schema({
        'type': 'object',
        'properties': {
            'is_active': {'type': 'boolean'},
        },
        'required': ['is_active'],
    })
    deployables.add_default_values({'is_active': False})

    products = ts.add_table('products')
    products.add_primary_key('organization_name,product_name')
    products.add_foreign_key('organization_name', 'organizations')
    products.add_schema({
        'type': 'object',
        'properties': {
            'product_name': {'pattern': r'^([a-z0-9-]){3,35}$'},
        },
    })

    tenant_names = ts.add_table('tenant_names')
    tenant_names.add_primary_key('tenant_name')
    tenant_names.add_foreign_key('organization_name,product_name', 'products')
    tenant_names.add_schema({
        'type': 'object',
        'properties': {
            'tenant_name': {'pattern': r'^([a-z0-9-]){3,20}$'},
            'reserved_at': {'format': 'date-time'},
            'reserved_by': {'type': 'string'},
        },
        'required': ['organization_name', 'product_name'],
    })

    tenants = ts.add_table('tenants')
    tenants.add_primary_key('tier_name,deployable_name,tenant_name')
    tenants.set_row_as_file(subfolder_name=tenants.name, group_by='tier_name,tenant_name')
    tenants.add_foreign_key('tier_name', 'tiers')
    tenants.add_foreign_key('deployable_name', 'deployable_names')
    tenants.add_foreign_key('tenant_name', 'tenant_names')
    tenants.add_schema({
        'type': 'object',
        'properties': {
            'description': {'type': 'string'},
            'state': {'enum': ['initializing', 'active', 'disabled', 'deleted']},
        },
    })
    tenants.add_default_values({'state': 'initializing'})

    public_keys = ts.add_table('public-keys')
    public_keys.set_row_as_file(subfolder_name='authentication')
    public_keys.add_primary_key('tier_name,deployable_name')
    public_keys.add_foreign_key('tier_name', 'tiers')
    public_keys.add_foreign_key('deployable_name', 'deployable_names')
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
    platforms.set_row_as_file(subfolder_name='authentication')
    platforms.add_primary_key('tier_name,deployable_name,tenant_name')
    platforms.add_foreign_key('tier_name', 'tiers')
    platforms.add_foreign_key('deployable_name', 'deployable_names')
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
        #self.file_store = FileBackend()
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


def get_domains(skip_errors=False):
    """Return all config domains stored on local disk."""
    config_folder = os.path.join(os.path.expanduser('~'), '.drift', 'config')
    domains = {}
    for dir_name in os.listdir(config_folder):
        path = os.path.join(config_folder, dir_name)
        if os.path.isdir(path):
            try:
                ts = get_store_from_url('file://' + path)
            except Exception as e:
                if skip_errors:
                    print "Note: '{}' is not a config folder, or is corrupt. ({}).".format(path, e)
                    continue
                else:
                    raise
            domain = ts.get_table('domain')
            domains[domain['domain_name']] = {'path': path, 'table_store': ts}
    return domains


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


