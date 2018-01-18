# -*- coding: utf-8 -*-
import os

from string import maketrans

from driftconfig.config import get_drift_table_store
from driftconfig.util import (
    set_sticky_config,
    register_this_deployable,
    register_this_deployable_on_tier,
    register_tier_defaults,
    get_tier_resource_modules,
    define_tenant,
    provision_tenant_resources,
)

DOMAIN_NAME = 'testdomain'
ORG_NAME = 'acme'
TIER_NAME = 'TIER'
DEPL_NAME = 'svc'
PROD_NAME = 'prod'
TENANT_NAME = 'test'

# If serial numbers need to be replaced with letters
tr_upper = maketrans('0123456789', 'ABCDEFGHIJ')
tr_lower = maketrans('0123456789', 'abcdefghij')


def _add(fn, ts, name, config_size, count, **kw):
    if count == 1:
        fn(ts, name, config_size, **kw)
    else:
        for i in xrange(count):
            fn(ts, '{}{}'.format(name, i), config_size, **kw)


def create_test_domain(config_size=None, resources=None, resource_attributes=None):
    """
    Creates a pre-populated Drift config to use for testing.
    'config_size' contains how many entries to create for each tables. The primary keys
    will have a serial number added at the end. See code for more info.
    'resources' is a list of resource module names to include in the config.
    Note that the resources will be provisioned as well. Pass in 'resource_attributes'
    as well for resource module initialization values.
    """
    config_size = config_size or {}
    resources = resources or []

    if resources:
        # Always assume local servers
        os.environ['DRIFT_USE_LOCAL_SERVERS'] = '1'

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
    _add(
        add_deployable, ts, DEPL_NAME, config_size, config_size['num_deployables'],
        resources=resources, resource_attributes=resource_attributes,
    )
    _add(add_organization, ts, ORG_NAME, config_size, config_size['num_org'])
    set_sticky_config(ts)

    for tenant in ts.get_table('tenant-names').find():
        provision_tenant_resources(ts=ts, tenant_name=tenant['tenant_name'])

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


def add_deployable(ts, deployable_name, config_size, resources, resource_attributes):
    package_info = {'name': deployable_name, 'description': deployable_name}

    register_this_deployable(
        ts=ts,
        package_info=package_info,
        resources=resources,
        resource_attributes=resource_attributes,
    )

    for tier in ts.get_table('tiers').find():
        register_this_deployable_on_tier(
            ts=ts,
            tier_name=tier['tier_name'],
            deployable_name=deployable_name,
        )

        resources = get_tier_resource_modules(
                ts=ts, tier_name=tier['tier_name'], skip_loading=False)
        register_tier_defaults(ts=ts, tier_name=tier['tier_name'], resources=resources)


def add_organization(ts, organization_name, config_size):
    ts.get_table('organizations').add({
        'organization_name': organization_name,
        'short_name': organization_name,
        'display_name': 'Some Test Organization',
    })

    # Each organization has x many products
    _add(
        add_product, ts, PROD_NAME, config_size, config_size['num_products'],
        organization_name=organization_name,
    )


def add_product(ts, product_name, config_size, organization_name):

    product_name = '{}-{}'.format(organization_name, product_name)
    deployables = [d['deployable_name'] for d in ts.get_table('deployables').find()]

    ts.get_table('products').add({
        'product_name': product_name,
        'organization_name': organization_name,
        'deployables': deployables,
    })

    # Each product has x many tenants
    _add(
        add_tenant, ts, TENANT_NAME, config_size, config_size['num_tenants'],
        product_name=product_name, organization_name=organization_name
    )


def add_tenant(ts, tenant_name_prefix, config_size, product_name, organization_name):

    for tier in ts.get_table('tiers').find():
        tenant_name = '{}-{}'.format(product_name, tenant_name_prefix)
        tier_name = tier['tier_name']
        if config_size['num_tiers'] != 1:
            tenant_name += '-{}'.format(tier_name.lower())

        # Define a tenant
        define_tenant(
            ts, tenant_name=tenant_name, product_name=product_name, tier_name=tier_name)


def get_name(which):
    """
    Returns the generated name of a tier, product, org, or tenant.
    """
    if which == 'organization':
        return ORG_NAME
    elif which == 'tier':
        return TIER_NAME
    elif which == 'deployable':
        return DEPL_NAME
    elif which == 'product':
        return '{}-{}'.format(ORG_NAME, PROD_NAME)
    elif which == 'tenant':
        return '{}-{}'.format(get_name('product'), TENANT_NAME)
