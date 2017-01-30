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
    is_active               boolean, default=false


products:
    product_name            string, pk
    organization_name       string, fk->organizations, required
    state                   enum initializing|active|disabled|deleted, default=active

tenant-names:
    # a tenant name is unique across all tiers
    tenant_name             string, pk
    product_name            string, fk->products, required
    organization_name       string, fk->organizations, required
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
        pub_rsa             string
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
import os
from datetime import datetime

from driftconfig.relib import TableStore, BackendError, get_store_from_url, copy_table_store, create_backend, load_meta_from_backend
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
            'state': {'enum': ['initializing', 'active', 'disabled', 'deleted']},
        },
        'required': ['organization_name'],
    })
    products.add_default_values({'state': 'active'})

    tenant_names = ts.add_table('tenant-names')
    tenant_names.add_primary_key('tenant_name')
    tenant_names.add_foreign_key('product_name', 'products')
    tenant_names.add_foreign_key('organization_name', 'organizations')
    tenant_names.add_schema({
        'type': 'object',
        'properties': {
            'tenant_name': {'pattern': r'^([a-z0-9-]){3,30}$'},
            'reserved_at': {'format': 'date-time'},
            'reserved_by': {'type': 'string'},
        },
        'required': ['product_name', 'organization_name'],
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

    '''
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
    '''
    users = ts.add_table('users')
    users.set_row_as_file(subfolder_name='authentication')
    users.add_primary_key('organization_name,user_name')
    users.add_foreign_key('organization_name', 'organizations')
    users.add_schema({
        'type': 'object',
        'properties': {
            'user_name': {'pattern': r'^([a-z0-9_]){2,30}$'},
            'create_date': {'format': 'date-time'},
            'valid_until': {'format': 'date-time'},
            'is_active': {'type': 'boolean'},
            'password': {'type': 'string'},
            'access_key': {'type': 'string'},
            'is_service': {'type': 'boolean'},
            'is_role_admin': {'type': 'boolean'},
            'access_key': {'type': 'string'},
        },
        'required': ['create_date', 'is_active', 'is_service', 'is_role_admin'],
    })
    users.add_default_values({
        'create_date': '@@utcnow', 'is_active': True,
        'is_service': False, 'is_role_admin': False
    })


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


    # API ROUTER STUFF - THIS SHOULDN'T REALLY BE IN THIS FILE HERE
    '''
    routing:
        tier_name               string, pk, fk->tiers
        deployable_name         string, pk, fk->deployables
        api                     string, required
        autoscaling
            min                 integer
            max                 integer
            desired             integer
            instance_type       string, required
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
        'required': ['product_name', 'in_use', 'key_type'],
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
    gameservers_instances.add_foreign_key('group_name,region', 'gameservers-machines')
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

    # END OF TABLE DEFS

    definition = ts.get_definition()
    new_ts = TableStore()
    new_ts.init_from_definition(definition)
    return new_ts


class Check():

    def __init__(self, ts):
        self._ts_original = ts

    def __enter__(self):
        self._ts_copy = copy_table_store(self._ts_original)
        return self._ts_copy

    def __exit__(self, *args):
        copy_table_store(self._ts_copy)


def push_to_origin(local_ts, force=False):
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
    return value contains 'reason' = 'checksum_differ' and 'time_diff' is
    the time difference between the changes.

    To force a push to a modified origin, set 'force' = True.
    """
    origin = local_ts.get_table('domain')['origin']
    origin_backend = create_backend(origin)
    try:
        origin_ts = load_meta_from_backend(origin_backend)
    except Exception as e:
        log.warning("Can't load meta info from %s: %s", origin_backend, repr(e))
        crc_match = True
        force = True
    else:
        crc_match = local_ts.meta['checksum'] == origin_ts.meta['checksum']

    if not force and not crc_match:
        local_modified = parse_8601(local_ts.meta['last_modified'])
        origin_modified = parse_8601(origin_ts.meta['last_modified'])
        return {'pushed': False, 'reason': 'checksum_differ', 'time_diff': origin_modified - local_modified}

    old, new = local_ts.refresh_metadata()

    if crc_match and old == new and not force:
        return {'pushed': True, 'reason': 'push_skipped_crc_match'}

    local_ts.save_to_backend(origin_backend)
    return {'pushed': True, 'reason': 'pushed_to_origin'}


def pull_from_origin(local_ts, ignore_if_modified=False, force=False):
    origin = local_ts.get_table('domain')['origin']
    origin_backend = create_backend(origin)
    origin_meta = load_meta_from_backend(origin_backend)
    old, new = local_ts.refresh_metadata()

    if old != new and not ignore_if_modified:
        return {'pulled': False, 'reason': 'local_is_modified'}

    crc_match = local_ts.meta['checksum'] == origin_meta.meta['checksum']
    if crc_match and not force:
        return {'pulled': True, 'table_store': local_ts, 'reason': 'pull_skipped_crc_match'}

    origin_ts = TableStore(origin_backend)
    return {'pulled': True, 'table_store': origin_ts, 'reason': 'pulled_from_origin'}


class TSTransactionError(RuntimeError):
    pass


class TSTransaction(object):
    def __init__(self, url=None):
        self._url = url
        self._ts = None

    def __enter__(self):
        if self._url:
            self._ts = get_store_from_url(self._url)
        else:
            domains = get_domains().values()
            if len(domains) != 1:
                raise RuntimeError("Can't figure out a single local table store: {}".format(d for d in domains))
            self._ts = domains[0]["table_store"]  # Assume 1 domain
            self._url = 'file://' + domains[0]['path']
            result = pull_from_origin(self._ts)
            if not result['pulled']:
                e = TSTransactionError("Can't pull latest table store: {}".format(result['reason']))
                e.result = result
                raise e
            self._ts =  result['table_store']

        return self._ts

    def __exit__(self, exc, value, traceback):
        if exc:
            return False

        result = push_to_origin(self._ts)
        if not result['pushed']:
            e = TSTransactionError("Can't push to origin: {}".format(result))
            e.result = result
            raise e

        # Write back to source
        source_backend = create_backend(self._url)
        self._ts.save_to_backend(source_backend)


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



