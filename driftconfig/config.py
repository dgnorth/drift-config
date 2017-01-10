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
    short_name              string, required
    display_name: string


tiers:
    tier_name               string, pk
    is_live                 boolean, default=true


deployable-names:
    deployable_name         string, pk
    display_name            string, required


deployables:
    tier_name               string, pk, fk->tiers
    deployable_name         string, pk, fk->deployables
    is_active               boolean, default=false


products:
    product_name            string, pk
    organization_name       string, fk->organizations


tenant-names:
    # a tenant name is unique across all tiers
    tenant_name             string, pk
    tier_name               string fk->tiers
    product_name            string fk->products
    organization_name       string fk->organizations
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


/////////////////////////////////////////////////////////////////////////////////
// APP SPECIFIC CONFIG
/////////////////////////////////////////////////////////////////////////////////

api-router/

routing:
    tier_name               string, pk, fk->tiers
    deployable_name         string, pk, fk->deployables
    api                     string, required
    autoscaling             dict
    release_version         string


keys:
    api_key_name            string, pk
    product_name            string, fk->products
    in_use                  boolean, default=true, required
    create_date             datetime, default=@@utcnow
    user_name               string


key-rules:
    rule_name               string, pk
    product_name            string, fk->products
    rule_type               enum pass|redirect|reject, required, default=pass
    reject:
        status_code             integer
        response_header         dict
        response_body           dict
    redirect:
        tenant_name             string


rule-assignments:
    api_key_name            string, fk->api-keys
    match_type              string, enum exact|partial
    version_pattern         string
    assignment_order        integer
    rule_name               string, fk->api-key-rules

    meta:
        required=api_key_name, match_type, version_pattern, assignment_order



NEW PRODUCT:
1. add product: dg-superkaiju
2. create tenant: dg-superkaiju
3. create api key: dg-superkaiju-8928973
4. create api keys for users: axl-8289374, matti-8237498, nonnib-8734234


this rule is always in play:
    organization for api key must match target organization.
    if not, RETURN 403 forbidden, org_mismatch

if no user rule specified, just let it slide.

if rules, but no rule match: RETURN 403 forbidden, version_not_allowed

RULES:
    0.6.1-bad       return rule_upgrade_superkaiju

    0.6.8           redirect rule_redirect_to_superkaiju_test

    .*-dev          pass
    .*-editor       pass
    .*-test         pass
    0.6.1-test      pass
    0.6.5           pass

    *               return rule_upgrade_superkaiju


version pattern:
    version from versions table (0.6.1, 0.6.5, 0.7.0, etc...)
    OR
    tag, which will be regex'd using find ("thetag", "dev", "editor", etc...)



KALEO WEB UIX WUNDER:

LIVENORTH SuperKaiju version rules:

REJECT:  3xx and 4xx response
    0.6.1-woohoo rule_message_to_woohoo
    0.6.1-bad    rule_upgrade_client

REDIRECT:
    0.7.0   tenant_name=superkaiju-test

PASS:
    .*-dev
    .*-editor
    .*-test
    0.6.*
    0.6.1-test

DEFAULT: rule_upgrade_superkaiju


from ue4 github a pretty json is fetched containing all the client actions:

actions:
upgrade_client
display_message



pk=superkaiju,dg-superkaiju-F00BAA12
version_pattern=*.-dev
pass=true

pk=superkaiju,dg-superkaiju-F00BAA12
version_pattern=*.-editor
pass=true

pk=superkaiju,dg-superkaiju-F00BAA12
version_pattern=0.6.1
pass=true

pk=superkaiju,dg-superkaiju-F00BAA12
version_pattern=0.6.1-test
pass=true

pk=superkaiju,dg-superkaiju-F00BAA12
version_pattern=*
pass=false
rule_name=pissoff






UIX USE CASE:

product page for SuperKaijuVR:

Pick tier: LIVENORTH

list of api keys:
    dg-superkaiju-B00B135





drift-base/

client-versions:
    product_name            string fk->products
    release_version         string



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
            'organization_name': {'pattern': r'^([a-z0-9]){2,20}$'},
            'short_name': {'pattern': r'^([a-z0-9]){2,20}$'},
            'display_name': {'type': 'string'},
        },
        'required': ['short_name'],
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

    deployable_names = ts.add_table('deployable-names')
    deployable_names.add_primary_key('deployable_name')
    deployable_names.add_schema({
        'type': 'object',
        'properties': {
            'deployable_name': {'pattern': r'^([a-z-]){3,20}$'},
            'display_name': {'type': 'string'},
        },
        'required': ['display_name'],
    })

    deployables = ts.add_table('deployables')
    deployables.add_primary_key('tier_name,deployable_name')
    deployables.add_foreign_key('tier_name', 'tiers')
    deployables.add_foreign_key('deployable_name', 'deployable-names')
    deployables.add_schema({
        'type': 'object',
        'properties': {
            'is_active': {'type': 'boolean'},
        },
        'required': ['is_active'],
    })
    deployables.add_default_values({'is_active': False})

    products = ts.add_table('products')
    products.add_primary_key('product_name')
    products.add_foreign_key('organization_name', 'organizations')
    products.add_schema({
        'type': 'object',
        'properties': {
            'product_name': {'pattern': r'^([a-z0-9-]){3,35}$'},
        },
    })

    tenant_names = ts.add_table('tenant-names')
    tenant_names.add_primary_key('tenant_name')
    tenant_names.add_foreign_key('product_name', 'products')
    tenant_names.add_foreign_key('organization_name', 'organizations')
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
    tenants.add_foreign_key('deployable_name', 'deployable-names')
    tenants.add_foreign_key('tenant_name', 'tenant-names')
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
    public_keys.add_foreign_key('deployable_name', 'deployable-names')
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
    platforms.add_foreign_key('deployable_name', 'deployable-names')
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

    
    # API ROUTER STUFF - THIS SHOULDN'T REALLY BE IN THIS FILE HERE
    '''
    routing:
        tier_name               string, pk, fk->tiers
        deployable_name         string, pk, fk->deployables
        api                     string, required
        autoscaling             dict
        release_version         string
    '''
    routing = ts.add_table('routing')
    routing.set_subfolder_name('api-router')
    routing.add_primary_key('tier_name,deployable_name')
    routing.add_schema({'type': 'object', 'properties': {
        'api': {'type': 'string'},
        'autoscaling': {'type': 'object', 'properties': {
            'min': {'type': 'integer'},
            'max': {'type': 'integer'},
            'desired': {'type': 'integer'},
            'instance_type': {'type': 'string'},
        }},
        'release_version': {'type': 'string'},
    }})


    '''
    keys:
        api_key_name            string, pk
        product_name            string, fk->products
        in_use                  boolean, default=true, required
        create_date             datetime, default=@@utcnow
        user_name               string
    '''
    keys = ts.add_table('api-keys')
    keys.set_subfolder_name('api-router')
    keys.add_primary_key('api_key_name')
    keys.add_foreign_key('product_name', 'products')
    keys.add_schema({
        'type': 'object', 
        'properties': 
        {
            'in_use': {'type': 'boolean'},
            'create_date': {'pattern': 'date-time'},
            'user_name': {'type': 'string'},
        },
        'required': ['in_use'],
    })
    keys.add_default_values({'in_use': True, 'create_date': '@@utcnow'})

    '''
    key-rules:
        rule_name               string, pk
        product_name            string, fk->products
        rule_type               enum pass|redirect|reject, required, default=pass
        reject:
            status_code             integer
            response_header         dict
            response_body           dict
        redirect:
            tenant_name             string
    '''
    keyrules = ts.add_table('api-key-rules')
    keyrules.set_subfolder_name('api-router')
    keyrules.add_primary_key('rule_name')
    keyrules.add_foreign_key('product_name', 'products')
    keyrules.add_schema({
        'type': 'object', 
        'properties': 
        {
            'rule_type': {'enum': ['pass', 'redirect', 'reject']},
            'reject': {'type': 'object', 'properties': {
                'status_code': {'type': 'integer'}, 
                'response_header': {'type': 'object'}, 
                'response_body': {'type': 'object'}, 
            }},
            'redirect': {'type': 'object', 'properties': {
                'tenant_name': {'type': 'string'}, 
            }},
        },
        'required': ['rule_type'],
    })
    keyrules.add_default_values({'rule_type': 'pass'})


    '''
    rule-assignments:
        api_key_name            string, fk->api-keys
        match_type              string, enum exact|partial
        version_pattern         string
        assignment_order        integer
        rule_name               string, fk->api-key-rules

        meta:
            required=api_key_name, match_type, version_pattern, assignment_order
    '''
    ruleass = ts.add_table('api-key-rule-assignments')
    ruleass.set_subfolder_name('api-router')
    ruleass.add_primary_key('api_key_name,match_type,version_pattern,assignment_order')
    ruleass.add_foreign_key('api_key_name', 'api-keys')
    ruleass.add_foreign_key('rule_name', 'api-key-rules')
    ruleass.add_schema({
        'type': 'object', 
        'properties': 
        {
            'match_type': {'enum': ['exact', 'partial']},
            'version_pattern': {'type': 'string'},
            'assignment_order': {'type': 'integer'},
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
        tenant = self['tenant-names'].get({'tenant_name': tenant_name})
        return tenant is not None

    def get_all_tenant_names(self):
        return [t['tenant_name'] for t in self['tenant-names'].find()]


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

    c['tenant-names'].add({'tenant_name': 'bloorgh'})
    c['tenants'].add({
        'tier_name','deployable_name','tenant_name'
        })
    print "existsts", c.tenant_exists('boo')
    print "all tenants", c.get_all_tenant_names()
    #c.save_config()


