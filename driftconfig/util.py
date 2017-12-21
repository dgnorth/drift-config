# -*- coding: utf-8 -*-
import logging
from datetime import datetime
import collections
import sys
import os
import os.path
import getpass

from driftconfig.relib import get_store_from_url, create_backend

log = logging.getLogger(__name__)


class ConfigNotFound(RuntimeError):
    """Raised if no config or multiple configs are found."""
    pass


class TenantNotConfigured(RuntimeError):
    """Raised in case a tenant is not found during a call to get_drift_config()."""
    pass


def config_dir(config_name, user_dir=False):
    """
    Return a path to config dir for 'config_name'.
    If 'user_dir' is set, the path is in the user directory, else it's in
    shared directory for all users.
    """
    # HACK: Use user dir always, except for windows
    if sys.platform == 'win32':
        user_dir = False
    else:
        user_dir = True

    if user_dir:
        return os.path.join(os.path.expanduser('~'), '.drift', 'config', config_name)
    else:
        if sys.platform == 'win32':
            root = os.environ.get('ProgramData')
        elif sys.platform == 'darwin':
            root = '/Library/Application Support'
        else:
            root = '/etc'

        return os.path.join(root, 'drift', 'config', config_name)


def get_domains(user_dir=False, skip_errors=False):
    """Return all config domains stored on local disk."""
    config_folder = config_dir('', user_dir=user_dir)
    domains = {}
    if not os.path.exists(config_folder):
        return {}

    for dir_name in os.listdir(config_folder):
        path = os.path.join(config_folder, dir_name)
        if os.path.isdir(path):
            try:
                ts = get_store_from_url('file://' + path)
            except Exception as e:
                if skip_errors:
                    log.warning("Note: '%s' is not a config folder or is corrupt. (%s).", path, e)
                    continue
                else:
                    raise
            domain = ts.get_table('domain')
            domains[domain['domain_name']] = {'path': path, 'table_store': ts}
    return domains


_sticky_ts = None


def set_sticky_config(ts):
    """Assign permanently 'ts' as the one and only drift config. Useful for tests."""
    global _sticky_ts
    _sticky_ts = ts


def get_default_drift_config():
    """
    Return Drift config as a table store.
    If 'DRIFT_CONFIG_URL' is found in environment variables, it is used to load the
    table store. If not found, the local disk is searched using get_domain() and if
    only one configuration is found there, it is used.
    If all else fails, this function raises an exception.

    If a table store object was set using set_sticky_config(), then that object will
    always be returned.
    """
    ts, source = get_default_drift_config_and_source()
    return ts


def get_default_drift_config_and_source():
    """
    Same as get_default_drift_config but returns a tuple of table store and the
    source of where it was loaded from.
    """
    if _sticky_ts:
        return _sticky_ts, 'memory://_dummy'

    url = os.environ.get('DRIFT_CONFIG_URL')
    if url:
        # Enable domain shorthand
        if ':' not in url:
            domains = get_domains()
            domain = domains.get(url)
            if domain:
                return domain['table_store'], 'file://' + domain['path']
            else:
                raise RuntimeError("No domain named '{}' found on local disk. Available domains: {}.".format(
                    url, ", ".join(domains.keys())))

        b = create_backend(url)
        return b.load_table_store(), url
    else:
        domains = get_domains()
        if len(domains) == 0:
            raise ConfigNotFound(
                "No config found in ~/.drift/config. Use 'driftconfig init' command to "
                "initialize a local config, or add a reference to the config using the "
                "environment variable 'DRIFT_CONFIG_URL'."
            )
        elif len(domains) != 1:
            domain_names = ", ".join(domains.keys())
            raise ConfigNotFound("Multiple Drift configurations found in ~/.drift/config.\n"
                "Specify which configuration to use by referencing it in the "
                "'DRIFT_CONFIG_URL' environment variable.\n"
                "Configurations available on local disk: %s."
                "" % domain_names)
        domain = domains.values()[0]
        return domain['table_store'], 'file://' + domain['path']


conf_tuple = collections.namedtuple(
    'driftconfig',
    [
        'table_store',
        'organization',
        'product',
        'tenant_name',
        'tier',
        'deployable',
        'tenant',
        'tenants',
        'domain',
        'drift_app',
        'source',
    ]
)


def get_drift_config(ts=None, tenant_name=None, tier_name=None, deployable_name=None, drift_app=None):
    """
    Return config tuple for given config context, containing the following properties:
    ['organization', 'product', 'tenant_name', 'tier', 'deployable', 'tenant']

    'ts' is the config TableStore object.
    'tenant_name' is the name of the tenant, or None if not applicable.
    """
    if ts:
        source = "internal"
    else:
        ts, source = get_default_drift_config_and_source()

    # Map tenant alias to actual tenant name if needed.
    tenant_name_row = ts.get_table('tenant-names').find({'alias': tenant_name})
    if tenant_name_row:
        tenant_name = tenant_name_row['tenant_name']

    # HACK BEGIN: Until 'flask_config' has been repurposed into 'drift_app' config, we enable a little mapping between
    # here for convenience:
    if drift_app and not deployable_name:
        deployable_name = drift_app['name']
    # HACK END

    tenants = ts.get_table('tenants')
    if tenant_name:
        tenant = tenants.get({'tier_name': tier_name, 'deployable_name': deployable_name, 'tenant_name': tenant_name})

        if not tenant:
            raise TenantNotConfigured(
                "Tenant '{}' not found for tier '{}' and deployable '{}'".format(tenant_name, tier_name, deployable_name)
            )
    else:
        tenant = None

    if tenant:
        tenant_name = tenants.get_foreign_row(tenant, 'tenant-names')
        product = ts.get_table('tenant-names').get_foreign_row(tenant_name, 'products')
        organization = ts.get_table('products').get_foreign_row(product, 'organizations')
    else:
        tenant_name = None
        product = None
        organization = None

    if tier_name and deployable_name:
        tenant_rows = tenants.find(
            {'tier_name': tier_name, 'deployable_name': deployable_name, 'state': 'active'}
        )
    else:
        tenant_rows = []

    conf = conf_tuple(
        table_store=ts,
        tenant=tenant,
        tier=ts.get_table('tiers').get({'tier_name': tier_name}),
        deployable=ts.get_table('deployables').get({'deployable_name': deployable_name, 'tier_name': tier_name}),
        domain=ts.get_table('domain'),
        tenant_name=tenant_name,
        tenants=tenant_rows,
        product=product,
        organization=organization,
        drift_app=drift_app,
        source=source,
    )

    # Make sure if tier name was specified, that it actually exists.
    if tier_name and conf.tier is None:
        raise RuntimeError(
            "Tier '{}' not found in config '{}'.".format(tier_name, conf.domain['domain_name']))

    return conf


def prepare_tenant_name(ts, tenant_name, product_name):
    """
    Prepares a tenant name by prefixing it with the organization shortname and returns the
    product and organization record associated with it. The value of 'tenant_name' may
    already contain the prefix.
    Returns a dict of 'tenant_name', 'product' and 'organization'.
    """
    products = ts.get_table('products')
    product = products.get({'product_name': product_name})
    if not product:
        raise RuntimeError("Product '{}' not found.".format(product_name))

    organization = products.get_foreign_row(product, 'organizations')

    if '-' in tenant_name:
        org_short_name = tenant_name.split('-', 1)[0]
        if org_short_name != organization['short_name']:
            raise RuntimeError("Tenant name '{}' must be prefixed with '{}'.".format(
                tenant_name, org_short_name)
            )
    else:
        tenant_name = '{}-{}'.format(organization['short_name'], tenant_name)

    return {
        'tenant_name': tenant_name,
        'product': product,
        'organization': organization,
    }


def define_tenant(ts, tenant_name, product_name, tier_name):
    """
    Defines a new tenant or updates/refreshes a current one.
    Table store 'ts' is updated accordingly.

    If 'tenant_name' does not exist, a new tenant record is created in 'tenant-names' table
    and new record is created in 'tenants' for each deployable that is enabled for
    the product identified by 'product_name'.

    If 'tenant_name' exists, and deployables for the product have changed (new one added or
    a current one removed), the 'tenants' table will be updated accordingly. In case of new
    deployable a new record will simply be added. In case a deployable is inactive or removed
    from the given product, the tenant record for the deployable will be put in state 'disabled'
    or 'deleted' respectively.

    Returns same dict as 'prepare_tenant_name' with addition of 'report' which contains a
    list of [deployable_name, state] tuples indicating state of each deployable for this
    tenant after the creation/update command.
    """
    prep = prepare_tenant_name(ts=ts, tenant_name=tenant_name, product_name=product_name)
    tenant_name = prep['tenant_name']
    product = prep['product']
    organization = prep['organization']

    # Add a record to 'tenant-names' if needed.
    tenant_names = ts.get_table('tenant-names')
    if not tenant_names.get({'tenant_name': tenant_name}):
        row = tenant_names.add({
            'tenant_name': tenant_name,
            'organization_name': organization['organization_name'],
            'product_name': product_name,
            'tier_name': tier_name,
            'reserved_by': getpass.getuser(),
            'reserved_at': datetime.utcnow().isoformat() + 'Z',
        })

    # Make a list of active and inactive deployables associated with the given product.
    active_deployables = []
    inactive_deployables = []
    deployables = ts.get_table('deployables')
    for deployable_name in product['deployables']:
        deployable = deployables.get({'tier_name': tier_name, 'deployable_name': deployable_name})
        if deployable:
            if deployable['is_active']:
                active_deployables.append(deployable_name)
            else:
                inactive_deployables.append(deployable_name)
        else:
            raise RuntimeError(
                "Deployable '{}' defined for product '{}' is not found in table 'deployables'.".format(
                    deployable_name, product_name)
            )

    tenants = ts.get_table('tenants')
    report = []  # List of deployable names and current state.

    def add_report(deployable_name, state):
        report_row = {'deployable_name': deployable_name, 'state': state}
        report.append(report_row)
        return report_row

    # Deactivate/delete deployables if needed
    for tenant in tenants.find({'tier_name': tier_name, 'tenant_name': tenant_name}):
        deployable_name = tenant['deployable_name']
        if deployable_name in inactive_deployables:
            tenant['state'] = 'disabled'
            add_report(deployable_name, 'disabled')
        elif deployable_name not in active_deployables:
            tenant['state'] = 'uninitializing'  # Signal de-provision of resources.
            add_report(deployable_name, 'uninitializing')

    # Activate/associate deployables if needed
    for deployable_name in active_deployables:
        pk = {
            'tier_name': tier_name,
            'deployable_name': deployable_name,
            'tenant_name': tenant_name
        }
        tenant = tenants.get(pk)
        if tenant:
            # If tenant isn't already active, signal it for provisioning of resources.
            if tenant['state'] != 'active':
                tenant['state'] = 'initializing'
                report_row = add_report(deployable_name, 'initializing')
            else:
                report_row = add_report(deployable_name, 'active')
        else:
            # State is set to 'initializing' by default signaling provisioning of resources.
            tenant = tenants.add(pk)
            report_row = add_report(deployable_name, 'initializing')

        # Initialize/update tenant resource config using default tier values.
        depl_names = ts.get_table('deployable-names').get({'deployable_name': deployable_name})
        tier = ts.get_table('tiers').get({'tier_name': tier_name})
        for resource_name in depl_names['resources']:
            # LEGACY SUPPORT: Shorten resource name to its last bit
            legacy_resource_name = resource_name.rsplit('.', 1)[1]

            # Update the tenants attributes but leave current ones intact
            for k, v in tier['resources'][resource_name].items():
                tenant[legacy_resource_name].setdefault(k, v)

            report_row.setdefault('resources', {})[resource_name] = tenant[legacy_resource_name]

    prep['report'] = report
    return prep


def refresh_tenants(ts, tenant_name=None, tier_name=None):
    """
    Refreshes config info for a tenant by calling define_tenant().
    If 'tenant_name' is set, only that tenant is refreshed.
    If 'tier_name' is set, only tenants defined on that tier are refreshed.
    """
    if tier_name:
        tiers = [tier_name]
    else:
        crit = {'tenant_name': tenant_name} if tenant_name else {}
        tiers = set(t['tier_name'] for t in ts.get_table('tenants').find(crit))

    if tenant_name:
        tenant_names = [tenant_name]
    else:
        crit = {'tier_name': tier_name} if tier_name else {}
        tenant_names = set(t['tenant_name'] for t in ts.get_table('tenants').find(crit))

    for tenant in ts.get_table('tenants').find():
        if tenant['tier_name'] not in tiers:
            continue
        if tenant['tenant_name'] not in tenant_names:
            continue

        tenant_info = ts.get_table('tenant-names').get({'tenant_name': tenant['tenant_name']})

        yield define_tenant(
            ts=ts,
            tenant_name=tenant['tenant_name'],
            product_name=tenant_info['product_name'],
            tier_name=tenant['tier_name']
        )
