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




global map rules for table files:
**********************************

foreign_key='organizations.organization_name'
template='organizations/{}'

ex:

table 'organizations'
organizations.json  # note, unchanged as the key field is pk, not fk.


table 'products'
organizations/nova/products.json
organizations/1939games/products.json

table 'tenant-names'
organizations/nova/tenant-names.json
organizations/1939games/tenant-names.json


table 'tenants'
organizations/nova/tenants/tenants.DEVNORTH.nova-live.json
organizations/1939games/tenants/tenants.DEVNORTH.1939-kards.json







drift-config core tables:


domain (single row):
    domain_name             string, required
    display_name            string
    origin                  string, required


organizations:
    organization_name       string, pk
    short_name              string, required
    display_name:           string
    state                   enum initializing|active|disabled|deleted, default=active


tiers:
    tier_name               string, pk
    is_live                 boolean, default=true
    state                   enum initializing|active|disabled|deleted, default=active


deployable-names:
    deployable_name         string, pk
    display_name            string, required


deployables:
    tier_name               string, pk, fk->tiers
    deployable_name         string, pk, fk->deployables
    version                 string
    is_active               boolean, default=false


products:
    product_name            string, pk
    organization_name       string, fk->organizations, required
    state                   enum initializing|active|disabled|deleted, default=active
    deployables             array of strings, required


tenant-names:
    # a tenant name is unique across all tiers
    tenant_name             string, pk
    product_name            string, fk->products, required
    organization_name       string, fk->organizations, required
    tier_name               string, pk, fk->tiers
    reserved_at             date-time, default=@@utcnow
    reserved_by             string


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

users:
    organization_name   string, pk, fk->organizations, required
    user_name           string, pk, required
    create_date         datetime, required, default=@@utcnow
    valid_until         datetime
    is_active           boolean, required, default=true
    password            string
    access_key          string
    is_service          boolean, required, default=false
    is_role_admin       boolean, required, default=false

    meta
        subfolder_name=authentication


/////////////////////////////////////////////////////////////////////////////////
// APP SPECIFIC CONFIG - api-router
/////////////////////////////////////////////////////////////////////////////////

api-router/

routing:
    tier_name               string, pk, fk->tiers
    deployable_name         string, pk, fk->deployables
    api                     string, required
    autoscaling             dict
    release_version         string


api-keys:
    api_key_name            string, pk
    product_name            string, fk->products, required
    in_use                  boolean, default=true, required
    create_date             datetime, default=@@utcnow
    key_type                enum product|custom, default=product, required
    custom_data             string


api-key-rules:
    product_name            string, pk, fk->products, required
    rule_name               string, pk

    assignment_order        integer, required
    version_patterns        array of strings, required
    is_active               boolean, required, default=true

    rule_type               enum pass|redirect|reject, required, default=pass
    response_header         dict
    redirect:
        tenant_name             string
    reject:
        status_code             integer
        response_body           dict


/////////////////////////////////////////////////////////////////////////////////
// APP SPECIFIC CONFIG - ue4-gameserver
/////////////////////////////////////////////////////////////////////////////////


ue4-gameservers/

ue4-gameservers-config (single row):
    build_archive_defaults
        region                  string
        bucket_name             string
        ue4_builds_folder       string


ue4-builds
    product_name            string, pk, fk->products, required

    s3_region               string
    bucket_name             string
    path                    string


gameservers-machines:
    product_name            string, pk, fk->products, required
    group_name              string, pk
    region                  string, pk
    platform                enum windows|linux, required
    autoscaling
        min                 integer
        max                 integer
        desired             integer
        instance_type       string, required


gameserver-instances:
    product_name            string, pk, fk->products, required
    group_name              string, pk, fk->gameservers-machines, required
    region                  string, pk, fk->gameservers-machines, required
    tenant_name             string, pk, fk->tenants, required
    ref:                    string, pk, required
    processes_per_machine   integer, required


/////////////////////////////////////////////////////////////////////////////////
// APP SPECIFIC CONFIG - drift-base
/////////////////////////////////////////////////////////////////////////////////

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
        public_key          string
        private_key         string      (note, this entry only accessible to the issuing service)

    authentications
        # 3rd party authentication
        tenant_name         fk->tenants
        provider_name       string
        provider_details    dict

users:
    # replacing the global 'service_user'
    organization_name   string, pk, fk->organizations, required
    user_name           string, pk, required
    create_date         datetime, required, default=@@utcnow
    valid_until         datetime
    is_active           boolean, required, default=true
    password            string
    access_key          string
    is_service          boolean, required, default=false
    is_role_admin       boolean, required, default=false


user-acl-roles:
    organization_name   string, pk, fk->organizations, required
    user_name           string, pk, fk->users, required
    role_name           string, pk, fk->user-roles, required
    tenant_name         string, fk->tenants


# dynamically populated by deployables during "init" phase
access-roles:
    role_name               string, pk
    deployable_name         string, fk->deployables
    description             string


use case:

tenant: superkaiju
login: directivegames.matti
pass:  bobo

-> lookup in user-acl:
    no record -> if 'superuser' in  org.role:
        assign ['admin'] to roles



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
from datetime import datetime

from driftconfig.relib import TableStore, create_backend
import driftconfig.relib
from driftconfig.util import get_default_drift_config_and_source
from driftconfig.backends import RedisBackend

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
    domain.add_default_values({'domain_name': '', 'origin': ''})

    organizations = ts.add_table('organizations')
    organizations.add_primary_key('organization_name')
    organizations.add_unique_constraint('short_name')
    organizations.add_schema({
        'type': 'object',
        'properties': {
            'organization_name': {'pattern': r'^([a-z0-9]){2,20}$'},
            'short_name': {'pattern': r'^([a-z0-9]){2,20}$'},
            'display_name': {'type': 'string'},
            'state': {'enum': ['initializing', 'active', 'disabled', 'deleted']},
        },
        'required': ['short_name'],
    })
    organizations.add_default_values({'state': 'active'})

    tiers = ts.add_table('tiers')
    tiers.add_primary_key('tier_name')
    tiers.add_schema({
        'type': 'object',
        'properties': {
            'tier_name': {'pattern': r'^([A-Z]){3,20}$'},
            'is_live': {'type': 'boolean'},
            'state': {'enum': ['initializing', 'active', 'disabled', 'deleted']},
        },
        'required': ['is_live'],
    })
    tiers.add_default_values({'is_live': True, 'state': 'active'})

    deployable_names = ts.add_table('deployable-names')
    deployable_names.add_primary_key('deployable_name')
    deployable_names.add_schema({
        'type': 'object',
        'properties': {
            'deployable_name': {'pattern': r'^[a-z]([a-z-0-9]){2,20}$'},
            'display_name': {'type': 'string'},
            'resources': {'type': 'array', 'item': 'string'},
            'resource_attributes': {'type': 'object'},
        },
        'required': ['display_name', 'resources'],
    })
    deployable_names.add_default_values({'resources': []})

    deployables = ts.add_table('deployables')
    deployables.add_primary_key('tier_name,deployable_name')
    deployables.add_foreign_key('tier_name', 'tiers')
    deployables.add_foreign_key('deployable_name', 'deployable-names')
    deployables.add_schema({
        'type': 'object',
        'properties': {
            'release': {'type': 'string'},
            'is_active': {'type': 'boolean'},
            'reason_inactive': {'type': 'string'},
        },
        'required': ['is_active'],
    })
    deployables.add_default_values({'is_active': True})

    products = ts.add_table('products')
    products.add_primary_key('product_name')
    products.add_foreign_key('organization_name', 'organizations')
    products.add_schema({
        'type': 'object',
        'properties': {
            'product_name': {'pattern': r'^([a-z0-9-]){3,35}$'},
            'state': {'enum': ['initializing', 'active', 'disabled', 'deleted']},
            'deployables': {'type': 'array', 'items': {'type': 'string'}},
        },
        'required': ['organization_name', 'deployables'],
    })
    products.add_default_values({'state': 'active', 'deployables': []})

    # Waiting a little bit with fixups of tenant-names and tenants table schema
    if "temporary fixy fix":
        tenant_names = ts.add_table('tenant-names')
        tenant_names.add_primary_key('tenant_name')
        tenant_names.add_foreign_key('product_name', 'products')
        tenant_names.add_foreign_key('organization_name', 'organizations')
        tenant_names.add_foreign_key('tier_name', 'tiers')
        tenant_names.add_unique_constraint('alias')
        tenant_names.add_schema({
            'type': 'object',
            'properties': {
                'tenant_name': {'pattern': r'^([a-z0-9-]){3,30}$'},
                'alias': {'pattern': r'^([a-z0-9-]){3,30}$'},
                'reserved_at': {'format': 'date-time'},
                'reserved_by': {'type': 'string'},
            },
            'required': ['product_name', 'organization_name', 'tier_name'],
        })
        tenant_names.add_default_values({'reserved_at': '@@utcnow'})

        tenants = ts.add_table('tenants')
        tenants.add_primary_key('tier_name,deployable_name,tenant_name')
        tenants.set_row_as_file(subfolder_name=tenants.name, group_by='tier_name,tenant_name')
        tenants.add_foreign_key('tier_name', 'tiers')
        tenants.add_foreign_key('deployable_name', 'deployable-names')
        tenants.add_foreign_key('tenant_name', 'tenant-names')
        tenants.add_schema({
            'type': 'object',
            'properties': {
                'state': {'enum': [
                    'initializing', 'active', 'disabled', 'uninitializing', 'deleted'
                ]},
            },
        })
        tenants.add_default_values({'state': 'initializing'})
    else:
        tenant_names = ts.add_table('tenant-names')
        tenant_names.add_primary_key('tenant_name,product_name')
        tenant_names.set_row_as_file(subfolder_name='tenants', group_by='product_name')
        tenant_names.add_unique_constraint('tenant_name')
        tenant_names.add_foreign_key('product_name', 'products')
        tenant_names.add_foreign_key('organization_name', 'organizations')
        tenant_names.add_foreign_key('tier_name', 'tiers')
        tenant_names.add_schema({
            'type': 'object',
            'properties': {
                'tenant_name': {'pattern': r'^([a-z0-9-]){3,30}$'},
                'reserved_at': {'format': 'date-time'},
                'reserved_by': {'type': 'string'},
            },
            'required': ['product_name', 'organization_name', 'tier_name'],
        })
        tenant_names.add_default_values({'reserved_at': '@@utcnow'})

        tenants = ts.add_table('tenants')
        tenants.add_primary_key('deployable_name,tenant_name')
        tenants.set_row_as_file(subfolder_name='tenants', group_by='tenant_name')
        tenants.add_foreign_key('deployable_name', 'deployable-names')
        tenants.add_foreign_key('tenant_name', 'tenant-names')
        tenants.add_schema({
            'type': 'object',
            'properties': {
                'state': {'enum': [
                    'initializing',  # Tenant resources are being provisioned
                    'active',  # Tenant resources are online and accessible
                    'disabled',  # Tenant resources are offline and not accessible
                    'uninitializing',  # Tenant resources are being unprovisoned/deleted.
                    'deleted',  # Tenant resources have been deleted.
                ]},
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
                    'public_key': {'type': 'string'},
                    'private_key': {'type': 'string'},
                },
            }},
        },
    })

    platforms = ts.add_table('platforms')
    platforms.set_row_as_file(subfolder_name='authentication')
    platforms.add_primary_key('product_name,provider_name')
    platforms.add_foreign_key('product_name', 'products')
    platforms.add_schema({
        'type': 'object',
        'properties': {
            'provider_details': {'type': 'object'},
        },
        'required': ['provider_details'],
    })

    '''
    users:
        user_name           string, pk, required
        tenant_name         string, pk, fk->tenants
        create_date         datetime, required, default=@@utcnow
        is_active           boolean, required, default=true
        meta
            subfolder_name=authentication
    '''
    users = ts.add_table('users')
    users.set_row_as_file(subfolder_name='authentication')
    users.add_primary_key('tenant_name,user_name')
    users.add_foreign_key('tenant_name', 'tenants')
    users.add_schema({
        'type': 'object',
        'properties': {
            'user_name': {'pattern': r'^([a-z0-9_]){2,30}$'},
            'create_date': {'format': 'date-time'},
            'is_active': {'type': 'boolean'},
        },
        'required': ['create_date', 'is_active'],
    })
    users.add_default_values({
        'create_date': '@@utcnow', 'is_active': True
    })

    '''
    access-keys:
        user_name           string pk, fk->users, required
        tenant_name         string pk, fk->users, required
        issue_date          datetime, required, default=@@utcnow
        access_key          string, required
    '''
    access_keys = ts.add_table('access-keys')
    access_keys.set_row_as_file(subfolder_name='authentication')
    access_keys.add_foreign_key('user_name,tenant_name', 'users')
    access_keys.add_schema({
        'type': 'object',
        'properties': {
            'issue_date': {'format': 'date-time'},
            'is_active': {'type': 'boolean'},
        },
        'required': ['issue_date', 'is_active'],
    })
    access_keys.add_default_values({
        'issue_date': '@@utcnow', 'is_active': True
    })

    '''
    client-credentials:
        user_name           string pk, fk->users, required
        tenant_name         string pk, fk-users, required
        create_date         datetime, required, default=@@utcnow
        client_id           string, required
        client_secret       string, required
    '''
    client_credentials = ts.add_table('client-credentials')
    client_credentials.set_row_as_file(subfolder_name='authentication')
    client_credentials.add_foreign_key('user_name, tenant_name', 'users')
    client_credentials.add_schema({
        'type': 'object',
        'properties': {
            'client_id': {'type': 'string'},
            'client_secret': {'type': 'string'},
        }
    })

    # The remaining two tables are currently not in use

    '''
    # dynamically populated by deployables during "init" phase
    access-roles:
        role_name               string, pk
        deployable_name         string, fk->deployables, required
        description             string
    '''
    access_roles = ts.add_table('access-roles')
    access_roles.set_row_as_file(subfolder_name='authentication')
    access_roles.add_primary_key('role_name')
    platforms.add_foreign_key('deployable_name', 'deployable-names')
    access_roles.add_schema({
        'type': 'object',
        'properties': {
            'description': {'type': 'string'},
        },
        'required': ['deployable_name'],
    })

    '''
    users-acl:
        organization_name   string, pk, fk->organizations, required
        user_name           string, pk, fk->users, required
        role_name           string, pk, fk->user-roles, required
        tenant_name         string, fk->tenants
    '''
    users_acl = ts.add_table('users-acl')
    users_acl.set_row_as_file(subfolder_name='authentication')
    users_acl.add_primary_key('organization_name,user_name,role_name')
    users_acl.add_foreign_key('organization_name', 'organizations')
    users_acl.add_foreign_key('user_name', 'users')
    users_acl.add_foreign_key('role_name', 'access-roles')
    users_acl.add_foreign_key('tenant_name', 'tenant-names')

    # RELEASE MANAGEMENT - THIS SHOULDN'T REALLY BE IN THIS FILE HERE, or what?
    '''
    instances:
        tier_name               string, pk, fk->tiers
        deployable_name         string, pk, fk->deployables
        autoscaling
            min                 integer
            max                 integer
            desired             integer
            instance_type       string, required
        release_version         string
    '''
    instances = ts.add_table('instances')
    instances.set_subfolder_name('release-mgmt')
    instances.add_primary_key('tier_name,deployable_name')
    instances.add_foreign_key('tier_name', 'tiers')
    instances.add_foreign_key('deployable_name', 'deployable-names')
    instances.add_schema({'type': 'object', 'properties': {
        'autoscaling': {'type': 'object', 'properties': {
            'min': {'type': 'integer'},
            'max': {'type': 'integer'},
            'desired': {'type': 'integer'},
            'instance_type': {'type': 'string'},
        }},
        'release_version': {'type': 'string'},
    }})

    # API ROUTER STUFF - THIS SHOULDN'T REALLY BE IN THIS FILE HERE
    '''
    nginx:
        tier_name               string, pk, fk->tiers
    '''
    nginx = ts.add_table('nginx')
    nginx.add_primary_key('tier_name')
    nginx.set_subfolder_name('api-router',)
    nginx.add_schema({'type': 'object', 'properties': {
        'worker_rlimit_nofile': {'type': 'integer'},
        'worker_connections': {'type': 'integer'},
        'api_key_passthrough':
        {
            'type': 'array',
            'items':
            {
                'type': 'object',
                'properties':
                {
                    'key_name': {'type': 'string'},
                    'key_value': {'type': 'string'},
                    'product_name': {'type': 'string'},
                },
                'required': ['key_name', 'key_value', 'product_name'],
            }
        },
    }})

    ''' a yaml representation would be:
    ---
    type: object
    properties:
      worker_connections: {type: integer}
      worker_rlimit_nofile: {type: integer}
      api_key_passthrough:
        type: array
        items:
          type: object
          required: [key_name, key_value ,product_name]
          properties:
            key_name: {type: string}
            key_value: {type: string}
            product_name: {type: string}

    '''

    '''
    routing:
        deployable_name         string, pk, fk->deployables
        api                     string, required
    '''
    routing = ts.add_table('routing')
    routing.set_subfolder_name('api-router')
    routing.add_primary_key('deployable_name')
    routing.add_foreign_key('deployable_name', 'deployable-names')
    routing.add_schema({
        'type': 'object',
        'properties':
        {
            'api': {'type': 'string'},
            'requires_api_key': {'type': 'boolean'},
        },
        'required': ['requires_api_key'],
    })
    routing.add_default_values({'requires_api_key': True})

    '''
    api-keys:
        api_key_name            string, pk
        product_name            string, fk->products, required
        in_use                  boolean, default=true, required
        create_date             datetime, default=@@utcnow
        key_type                enum product|custom, default=product, required
        custom_data             string
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
            'create_date': {'format': 'date-time'},
            'key_type': {'enum': ['product', 'custom']},
            'custom_data': {'type': 'string'},
        },
        'required': ['in_use', 'key_type'],
    })
    keys.add_default_values({'in_use': True, 'create_date': '@@utcnow', 'key_type': 'product'})

    '''
    api-key-rules:
        product_name            string, pk, fk->products
        rule_name               string, pk

        assignment_order        integer, required
        version_patterns        array of strings, required
        is_active               boolean, required, default=true

        rule_type               enum pass|redirect|reject, required, default=pass
        response_header         dict
        redirect:
            tenant_name             string
        reject:
            status_code             integer
            response_body           dict
    '''
    keyrules = ts.add_table('api-key-rules')
    keyrules.set_subfolder_name('api-router')
    keyrules.add_primary_key('product_name,rule_name')
    keyrules.add_foreign_key('product_name', 'products')
    keyrules.add_schema({
        'type': 'object',
        'properties':
        {
            'assignment_order': {'type': 'integer'},
            'version_patterns': {'type': 'array', 'items': {'type': 'string'}},
            'is_active': {'type': 'boolean'},

            'rule_type': {'enum': ['pass', 'redirect', 'reject']},

            'response_header': {'type': 'object'},
            'redirect': {'type': 'object', 'properties': {
                'tenant_name': {'type': 'string'},
            }},
            'reject': {'type': 'object', 'properties': {
                'status_code': {'type': 'integer'},
                'response_body': {'type': 'object'},
            }},
        },
        'required': ['assignment_order', 'version_patterns', 'is_active', 'rule_type'],
    })
    keyrules.add_default_values({'is_active': True, 'rule_type': 'pass'})

    '''
    ue4-gameservers/

    ue4-gameservers-config (single row):
        build_archive_defaults
            region                  string
            bucket_name             string
            ue4_builds_folder       string
    '''
    ue4_gameservers_config = ts.add_table('ue4-gameservers-config', single_row=True)
    ue4_gameservers_config.set_subfolder_name('ue4-gameservers')
    ue4_gameservers_config.add_schema({
        'type': 'object',
        'properties':
        {
            'build_archive_defaults': {
                'type': 'object',
                'properties':
                {
                    'region': {'type': 'string'},
                    'bucket_name': {'type': 'string'},
                    'ue4_builds_folder': {'type': 'string'},
                },
            },
        },
    })

    '''
    ue4-build-artifacts
        product_name            string, pk, fk->products, required

        s3_region               string
        bucket_name             string
        path                    string
        command_line            string
    '''
    ue4_build_artifacts = ts.add_table('ue4-build-artifacts')
    ue4_build_artifacts.set_subfolder_name('ue4-gameservers')
    ue4_build_artifacts.add_primary_key('product_name')
    ue4_build_artifacts.add_foreign_key('product_name', 'products')
    ue4_build_artifacts.add_schema({
        'type': 'object',
        'properties':
        {
            's3_region': {'type': 'string'},
            'bucket_name': {'type': 'string'},
            'path': {'type': 'string'},
            'command_line': {'type': 'string'},
        },
        'required': ['s3_region', 'bucket_name', 'path', 'command_line'],
    })

    '''
    gameservers-machines:
        product_name            string, pk, fk->products, required
        group_name              string, pk
        region                  string, pk
        platform                enum windows|linux, required
        autoscaling
            min                 integer
            max                 integer
            desired             integer
            instance_type       string, required
    '''
    gameservers_machines = ts.add_table('gameservers-machines')
    gameservers_machines.set_subfolder_name('ue4-gameservers')
    gameservers_machines.add_primary_key('product_name,group_name,region')
    gameservers_machines.add_foreign_key('product_name', 'products')
    gameservers_machines.add_schema({
        'type': 'object',
        'properties':
        {
            'region': {'type': 'string'},
            'platform': {'enum': ['windows', 'linux']},
            'autoscaling': {'type': 'object', 'properties': {
                'min': {'type': 'integer'},
                'max': {'type': 'integer'},
                'desired': {'type': 'integer'},
                'instance_type': {'type': 'string'},
            }},
        },
    })

    '''
    gameservers-instances:
        gameserver_instance_id  string, pk, default=@@identity
        product_name            string, fk->products, required
        group_name              string, fk->gameservers-machines, required
        region                  string, fk->gameservers-machines, required
        tenant_name             string, fk->tenants, required
        ref                     string, required
        processes_per_machine   integer, required
        command_line            string
    '''
    gameservers_instances = ts.add_table('gameservers-instances')
    gameservers_instances.set_subfolder_name('ue4-gameservers')
    gameservers_instances.add_primary_key('gameserver_instance_id')
    gameservers_instances.add_foreign_key('product_name', 'products')
    # gameservers_instances.add_foreign_key('group_name,region', 'gameservers-machines')
    gameservers_instances.add_foreign_key('tenant_name', 'tenant-names')
    gameservers_instances.add_schema({
        'type': 'object',
        'properties':
        {
            'ref': {'type': 'string'},
            'processes_per_machine': {'type': 'integer'},
            'command_line': {'type': 'string'},
        },
        'required': ['product_name', 'group_name', 'region', 'tenant_name', 'ref', 'processes_per_machine'],
    })
    gameservers_instances.add_default_values({'gameserver_instance_id': '@@identity'})

    '''
    metrics:
        tier_name               string, pk, fk->tiers
        deployable_name         string, pk, fk->deployables
    '''
    metrics = ts.add_table('metrics')
    metrics.add_primary_key('tenant_name')
    metrics.add_foreign_key('tenant_name', 'tenant-names')
    metrics.add_schema({
        'type': 'object',
        'properties':
        {
            's3_bucket': {'type': 'string'},
        },
        'required': ['s3_bucket'],
    })

    # END OF TABLE DEFS

    definition = ts.get_definition()
    new_ts = TableStore()
    new_ts.init_from_definition(definition)
    return new_ts


def push_to_origin(local_ts, force=False, _first=False, _origin_crc=None):
    """
    Pushed 'local_ts' to origin.
    Returns a dict with 'pushed' as True or False depending on success.

    If local store has not been modified since last pull, and the origin
    has the same version, no upload is actually performed and the return
    value contains 'reason' = 'push_skipped_crc_match'.

    If local store has indeed been modified since last pull, but the origin
    has stayed unchanged, upload is performed and the return value contains
    'reason' = 'pushed_to_origin'.

    If origin has changed since last pull, the push is cancelled and the
    return value contains 'reason' = 'checksum_differ'.

    To force a push to a modified origin, set 'force' = True.

    If 'skip_cache' is true, the cache, if defined for the table store, will
    not be updated.

    '_first' is used internally, and indicates it's the first time the table
    store is pushed.

    '_origin_crc' is the original and expected origin crc.
    """
    origin = local_ts.get_table('domain')['origin']
    origin_backend = create_backend(origin)
    origin_ts = None

    if _first:
        crc_match = force = True
    else:
        try:
            origin_ts = origin_backend.load_table_store()
        except Exception as e:
            log.warning("Can't load table store from %s: %s", origin_backend, repr(e))
            crc_match = force = True
        else:
            expected_crc = _origin_crc or local_ts.meta['checksum']
            crc_match = expected_crc == origin_ts.meta['checksum']

    if not force and not crc_match:
        return {
            'pushed': False,
            'reason': 'checksum_differ',
            'local_meta': local_ts.meta.get(),
            'origin_meta': origin_ts.meta.get(),
            'expected_crc': expected_crc,
            # 'local_ts': local_ts,
            # 'origin_ts': origin_ts,
        }

    old, new = local_ts.refresh_metadata()

    if origin_ts is not None:
        crc_match = local_ts.meta['checksum'] == origin_ts.meta['checksum']

    if crc_match and old == new and not force:
        return {'pushed': True, 'reason': 'push_skipped_crc_match'}

    # Always turn on all integrity check when saving to origin
    tmp = driftconfig.relib.CHECK_INTEGRITY
    driftconfig.relib.CHECK_INTEGRITY = ['pk', 'fk', 'unique', 'schema', 'constraints']
    try:
        origin_backend.save_table_store(local_ts)
    finally:
        driftconfig.relib.CHECK_INTEGRITY = tmp

    return {'pushed': True, 'reason': 'pushed_to_origin'}


def pull_from_origin(local_ts, ignore_if_modified=False, force=False):
    origin = local_ts.get_table('domain')['origin']
    origin_ts = create_backend(origin).load_table_store()
    old, new = local_ts.refresh_metadata()

    if old != new and not ignore_if_modified:
        return {'pulled': False, 'reason': 'local_is_modified'}

    crc_match = local_ts.meta['checksum'] == origin_ts.meta['checksum']
    if crc_match and not force:
        return {'pulled': True, 'table_store': local_ts, 'reason': 'pull_skipped_crc_match'}

    return {'pulled': True, 'table_store': origin_ts, 'reason': 'pulled_from_origin'}


def get_redis_cache_backend(ts, tier_name):
    """Returns cache backend for tier 'tier_name' in 'ts'."""
    # Note: drift.core.resources.redis module defines where to find default
    # connection information for a Redis server. We make good use of that here,
    # but it does mean that this piece of code below is now coupled with
    # aforementioned module.
    # Returns None if no cache is defined for the tier and none can be assumed from
    # redis config.
    # TODO: Fix bad coupling as described above.
    domain = ts.get_table('domain').get()
    tier = ts.get_table('tiers').get({'tier_name': tier_name})
    if 'cache' in tier:
        b = create_backend(tier['cache'])
    else:
        if 'resources' not in tier or 'drift.core.resources.redis' not in tier['resources']:
            return None
        redis_info = tier['resources']['drift.core.resources.redis']
        b = RedisBackend.create_from_server_info(
            host=redis_info['host'],
            port=redis_info['port'],
            domain_name=domain['domain_name'],
        )

    return b


def update_cache(ts, tier_name):
    """Push table store 'ts' to its designated Redis cache on tier 'tier_name'."""

    b = get_redis_cache_backend(ts, tier_name)
    if b:
        b.save_table_store(ts)
    return b


class TSTransactionError(RuntimeError):
    pass


class TSTransaction(object):
    _semaphore = 0

    def __init__(self, commit_to_origin=True, write_to_scratch=True):
        self._commit_to_origin = commit_to_origin
        self._write_to_scratch = write_to_scratch
        self._ts = None

    def __enter__(self):
        if self._semaphore != 0:
            raise RuntimeError("Can't nest TSTransactions")

        self._semaphore = 1
        ts, self._url = get_default_drift_config_and_source()
        result = pull_from_origin(ts)
        if not result['pulled']:
            e = TSTransactionError("Can't pull latest table store: {}".format(result['reason']))
            e.result = result
            raise e
        self._ts = result['table_store']
        self._origin_crc = self._ts.meta['checksum']
        self._ts._lock_meta = True

        return self._ts

    def __exit__(self, exc, value, traceback):
        self._semaphore = 0
        self._ts._lock_meta = False

        if exc:
            return False

        if self._commit_to_origin:
            result = push_to_origin(self._ts, _origin_crc=self._origin_crc)
            if not result['pushed']:
                e = TSTransactionError("Can't push to origin: {}".format(result))
                e.result = result
                raise e

        if self._write_to_scratch:
            # Update cache if applicable
            source_backend = create_backend(self._url)
            source_backend.save_table_store(self._ts)


class TSLocal(object):
    def __init__(self):
        self._ts = None

    def __enter__(self):
        self._ts, self._url = get_default_drift_config_and_source()
        return self._ts

    def __exit__(self, exc, value, traceback):
        if exc:
            return False

        # Update cache if applicable
        source_backend = create_backend(self._url)
        source_backend.save_table_store(self._ts)


def parse_8601(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")


def _(d):
    import json
    return json.dumps(d, indent=4, sort_keys=True)


if __name__ == '__main__':
    logging.basicConfig(level='INFO')

    # test TSTransaction

    with TSTransaction('file://~/.drift/config/dgnorth') as ts:
        ts.get_table('domain').get()['display_name'] += ' bluuu!'
