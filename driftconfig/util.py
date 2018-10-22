# -*- coding: utf-8 -*-
import logging
from datetime import datetime
import collections
import sys
import os
import os.path
import getpass
import importlib

from click import echo

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
            raise ConfigNotFound(
                "Multiple Drift configurations found in ~/.drift/config.\n"
                "Specify which configuration to use by referencing it in the "
                "'DRIFT_CONFIG_URL' environment variable.\n"
                "Configurations available on local disk: %s."
                "" % domain_names)
        domain = next(d for d in domains.values())
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


def get_drift_config(
    ts=None,
    tenant_name=None,
    tier_name=None,
    deployable_name=None,
    drift_app=None,
    allow_missing_tenant=False
):
    """
    Return config tuple for given config context, containing the following properties:
    ['organization', 'product', 'tenant_name', 'tier', 'deployable', 'tenant']

    'ts' is the config TableStore object.
    'tenant_name' is the name of the tenant, or None if not applicable.

    If 'tenant_name' is specified but not found in config a TenantNotConfigured exception
    is raised. If 'allow_missing_tenant' is True however, then the config tuple will be
    returned but with the 'tenant' property set to None.
    """
    if ts:
        source = "internal"
    else:
        ts, source = get_default_drift_config_and_source()

    # Map tenant alias to actual tenant name if needed.
    tenant_name_row = ts.get_table('tenant-names').find({'alias': tenant_name})
    alias = tenant_name
    if tenant_name_row:
        tenant_name = tenant_name_row[0]['tenant_name']
        alias = "{} (alias={})".format(tenant_name, alias)

    # HACK BEGIN: Until 'flask_config' has been repurposed into 'drift_app' config, we enable a little mapping between
    # here for convenience:
    if drift_app and not deployable_name:
        deployable_name = drift_app['name']
    # HACK END

    tenants = ts.get_table('tenants')
    if tenant_name:
        tenant = tenants.get({'tier_name': tier_name, 'deployable_name': deployable_name, 'tenant_name': tenant_name})

        if not tenant and not allow_missing_tenant:
            raise TenantNotConfigured(
                "Tenant '{}' not found for tier '{}' and deployable '{}'".format(alias, tier_name, deployable_name)
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
    tenant_master_row = tenant_names.get({'tenant_name': tenant_name})
    if not tenant_master_row:
        tenant_master_row = tenant_names.add({
            'tenant_name': tenant_name,
            'organization_name': organization['organization_name'],
            'product_name': product_name,
            'tier_name': tier_name,
            'reserved_by': getpass.getuser(),
            'reserved_at': datetime.utcnow().isoformat() + 'Z',
        })

    prep['tenant_master_row'] = tenant_master_row

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
                "Deployable '{}' defined for product '{}' is not found in table 'deployables' for tier {}.".format(
                    deployable_name, product_name, tier_name)
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
        elif deployable_name not in active_deployables and tenant['state'] != 'deleted':
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
            report_row = add_report(deployable_name, tenant['state'])
        else:
            # State is set to 'initializing' by default signaling provisioning of resources.
            tenant = tenants.add(pk)
            report_row = add_report(deployable_name, 'initializing')

        # Initialize/update tenant resource config using default tier or deployables values.
        depl_names = ts.get_table('deployable-names').get({'deployable_name': deployable_name})
        tier = ts.get_table('tiers').get({'tier_name': tier_name})
        for resource_name in depl_names['resources']:
            # LEGACY SUPPORT: Shorten resource name to its last bit
            legacy_resource_name = resource_name.rsplit('.', 1)[1]

            # Update the tenants attributes but leave current ones intact
            resource_attribs = tenant.setdefault(legacy_resource_name, {})

            # Apply tier defaults
            for k, v in tier['resources'][resource_name].items():
                resource_attribs.setdefault(k, v)

            # Apply deployable defaults
            deployable_defaults = depl_names['resource_attributes'].get(resource_name, {})
            for k, v in deployable_defaults.items():
                resource_attribs.setdefault(k, v)

            report_row.setdefault('resources', {})[resource_name] = resource_attribs

    prep['report'] = report
    return prep


def provision_tenant_resources(ts, tenant_name, deployable_name=None, preview=False):
    """
    Calls resource provisioning functions for tenant 'tenant_name'.
    If 'deployable_name' is set, only the resource modules for that deployable
    are called.
    If 'preview' then the provision_resource() callback function is not called.
    """
    tenant_info = ts.get_table('tenant-names').get({'tenant_name': tenant_name})
    termination_protection = tenant_info.get('termination_protection', False)
    crit = {'tenant_name': tenant_name, 'tier_name': tenant_info['tier_name']}
    if deployable_name:
        crit['deployable_name'] = deployable_name
    configurations = ts.get_table('tenants').find(crit)

    report = {
        'tenant': tenant_info,
        'deployables': {},
    }

    log.info("Provisioning tenant '%s'", tenant_name)

    for tenant_config in configurations:
        # LEGACY SUPPORT: Need to look up the actual resource module name
        depl = ts.get_table('deployable-names').get({'deployable_name': tenant_config['deployable_name']})
        depl_report = {
            'resources': {}
        }
        report['deployables'][tenant_config['deployable_name']] = depl_report

        log.info("  Deployable: '%s'", tenant_config['deployable_name'])

        # Termination protection is specified for the tenant as a whole, but we need to see if
        # individual deployable is uninitializing and stop it there.
        if tenant_config['state'] == 'uninitializing' and termination_protection:
            depl_report['error'] = "Tenant has termination protection. Can't uninitialize."
            continue

        for dryrun in [True, False]:
            for resource_module in depl['resources']:
                legacy_resource_name = resource_module.rsplit('.', 1)[1]
                resource_attributes = tenant_config.setdefault(legacy_resource_name, {})

                def report_error():
                    result = [
                        "Failed to provision resource '{}'.\n{}: {}\nAttributes: {}".format(
                            resource_module, e.__class__.__name__, e, resource_attributes)
                        ]
                    depl_report['resources'][resource_module] = result

                try:
                    m = importlib.import_module(resource_module)
                except Exception as e:
                    report_error()
                    continue

                has_provision_method = hasattr(m, 'provision_resource')

                if has_provision_method:
                    log.info("  -> %s", resource_module)

                if dryrun:  # Just verifying that all the resource modules can load properly
                    if hasattr(m, 'provision_resource_precheck'):
                        m.provision_resource_precheck()
                    continue

                if has_provision_method and not preview:
                    try:
                        result = m.provision_resource(
                            ts=ts,
                            tenant_config=tenant_config,
                            attributes=resource_attributes,
                        )
                        depl_report['resources'][resource_module] = result
                    except Exception as e:
                        report_error()
                else:
                    depl_report['resources'][resource_module] = resource_attributes

        depl_report['old_state'] = tenant_config['state']

        if tenant_config['state'] == 'initializing':
            tenant_config['state'] = 'active'
        elif tenant_config['state'] == 'uninitializing':
            tenant_config['state'] = 'deleted'

        depl_report['new_state'] = tenant_config['state']

    return report


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


# NOTE THIS IS DEPRECATED FUNCTION AND NEEDS TO BE UPGRADED TO NU STYLE SUMTHIN
def get_parameters(config, args, required_keys, resource_name):
    raise NotImplementedError()
    echo("ROUESR IDC NOAME IS " + resource_name)
    defaults = config.tier.get('resource_defaults', [])

    # gather default parameters from tier
    params = {}
    for default_params in defaults:
        if default_params.get("resource_name") == resource_name:
            params = default_params["parameters"].copy()

    if not params:
        raise RuntimeError("No provisioning defaults in tier config for '%s'. Cannot continue" % resource_name)
    if set(required_keys) != set(params.keys()):
        log.error("%s vs %s" % (required_keys, params.keys()))
        raise RuntimeError("Tier provisioning parameters do not match tier defaults for '%s'. Cannot continue" % resource_name)
    for k in args.keys():
        if k not in params:
            raise RuntimeError("Custom parameter %s for '%s' not supported. Cannot continue" % (k, resource_name))

    log.info("Default parameters for '%s' from tier: %s", resource_name, params)
    params.update(args)
    if args:
        log.info("Connection info for '%s' with custom parameters: %s", resource_name, params)
    return params


def register_this_deployable(ts, package_info, resources, resource_attributes):
    """
    Registers top level information for a deployable package.

    'package_info' is a dict containing Python package info for current deployable. The dict contains
    at a minimum 'name' and 'description' fields.

    'resources' is a list of resource modules in use by this deployable.

    'resource_attributes' is a dict containing optional attributes for resources. Key is the resource
    module name, value is a dict of any key values attributes which gets passed into registration callback
    functions in the resource modules.

    Returns a dict with 'old_registration' row and 'new_registration' row.
    """
    tbl = ts.get_table('deployable-names')
    pk = {'deployable_name': package_info['name']}
    orig_row = tbl.get(pk)
    if orig_row:
        row = orig_row
        orig_row = orig_row.copy()
    else:
        pk['display_name'] = package_info['description']  # This is a required field
        row = tbl.add(pk)

    row['display_name'] = package_info['description']
    if 'long-description' in package_info and package_info['long-description'] != "UNKNOWN":
        row['description'] = package_info['long-description']
    row['resources'] = resources
    row['resource_attributes'] = resource_attributes

    # Call hooks for top level registration.
    for module_name in row['resources']:
        m = importlib.import_module(module_name)
        if hasattr(m, 'register_deployable'):
            attributes = resource_attributes.setdefault(module_name, {})
            m.register_deployable(
                ts=ts,
                deployablename=row,
                attributes=attributes,
            )

            # For cleanliness, remove resource attribute entry if it's empty
            if not attributes:
                del resource_attributes[module_name]

    return {'old_registration': orig_row, 'new_registration': row}


def register_this_deployable_on_tier(ts, tier_name, deployable_name):
    """
    Registers tier specific info for a deployable package.

    Returns a dict with 'old_registration' row and 'new_registration' row.
    """
    tbl = ts.get_table('deployables')
    pk = {'tier_name': tier_name, 'deployable_name': deployable_name}
    orig_row = tbl.get(pk)
    if orig_row:
        row, orig_row = orig_row, orig_row.copy()
    else:
        row = tbl.add(pk)

    registration_row = ts.get_table('deployable-names').get({'deployable_name': deployable_name})
    resource_attributes = registration_row['resource_attributes']

    # Call hooks for tier registration info.
    for module_name in registration_row['resources']:
        m = importlib.import_module(module_name)
        if hasattr(m, 'register_deployable_on_tier'):
            attributes = resource_attributes.setdefault(module_name, {})
            m.register_deployable_on_tier(
                ts=ts,
                deployable=row,
                attributes=attributes,
            )

            # For cleanliness, remove resource attribute entry if it's empty
            if not attributes:
                del resource_attributes[module_name]

    return {'old_registration': orig_row, 'new_registration': row}


def get_tier_resource_modules(ts, tier_name, skip_loading=False, ignore_import_errors=False):
    """
    Returns a list of all resource modules registered on 'tier_name'.
    Each entry is a dict with 'module_name', 'module' and 'default_attributes'.
    If 'skip_loading' the 'module' value is None.
    If 'ignore_import_errors' import errors will be ignored.
    """
    resources = set()
    deployables = ts.get_table('deployables')
    for deployable in deployables.find({'tier_name': tier_name}):
        row = deployables.get_foreign_row(deployable, 'deployable-names')
        resources |= set(row.get('resources', []))  # Update resource module list

    modules = []
    for module_name in resources:
        if skip_loading:
            m = None
        else:
            try:
                m = importlib.import_module(module_name)
            except ImportError as e:
                if not ignore_import_errors:
                    raise
                log.warning("Ignoring import error: %s", e)
                continue
        modules.append({
            'module_name': module_name,
            'module': m,
            'default_attributes': getattr(m, 'TIER_DEFAULTS', {}),
        })

    return modules


def register_tier_defaults(ts, tier_name, resources=None):
    """
    Registers tier specific default values for resources.
    Note, this is deployable-agnostic info.
    If 'resources' is set, it must originate from get_tier_resource_modules(). This is to
    give the option of modifying values in 'default_attributes'.
    """
    # Enumerate all resource modules from all registered deployables on this tier,
    # configure default vaules and call hooks for tier registration info.
    tier = ts.get_table('tiers').get({'tier_name': tier_name})
    module_resources = resources or get_tier_resource_modules(ts=ts, tier_name=tier_name)
    config_resources = tier.setdefault('resources', {})

    for resource in module_resources:
        # Create or refresh default attributes entry in config
        attributes = config_resources.setdefault(resource['module_name'], {})
        # Only add new entries to 'attributes' otherwise we override some values with placeholder data.
        for k, v in resource['default_attributes'].items():
            if k not in attributes or attributes[k] == "<PLEASE FILL IN>":
                attributes[k] = v

        # Give resource module chance to do custom work
        if hasattr(resource['module'], 'register_resource_on_tier'):
            resource['module'].register_resource_on_tier(
                ts=ts,
                tier=tier,
                attributes=attributes,
            )
