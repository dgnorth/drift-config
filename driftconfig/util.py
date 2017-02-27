# -*- coding: utf-8 -*-
import logging
from datetime import datetime
import collections
import sys
import os
import os.path

from driftconfig.relib import get_store_from_url, create_backend

log = logging.getLogger(__name__)


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
                    print "Note: '{}' is not a config folder, or is corrupt. ({}).".format(path, e)
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
        return _sticky_ts

    url = os.environ.get('DRIFT_CONFIG_URL')
    if url:
        b = create_backend(url)
        return b.load_table_store(), url
    else:
        domains = get_domains()
        if len(domains) == 0:
            raise RuntimeError(
                "No config found in ~/.drift/config. Use 'driftconfig init' command to "
                "initialize a local config, or add a reference to the config using the "
                "environment variable 'DRIFT_CONFIG_URL'."
            )
        elif len(domains) != 1:
            raise RuntimeError("No single candidate found in ~/.drift/config")
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
        'drift_app'
    ]
)


def get_drift_config(ts=None, tenant_name=None, tier_name=None, deployable_name=None, drift_app=None):
    """
    Return config tuple for given config context, containing the following properties:
    ['organization', 'product', 'tenant_name', 'tier', 'deployable', 'tenant']

    'ts' is the config TableStore object.
    'tenant_name' is the name of the tenant, or None if not applicable.
    """
    ts = ts or get_default_drift_config()
    tenants = ts.get_table('tenants')

    # HACK: Until 'flask_config' has been repurposed into 'drift_app' config, we enable a little mapping between
    # here for convenience:
    if drift_app and not deployable_name:
        deployable_name = drift_app['name']

    if tenant_name:
        tenant = tenants.get({'tier_name': tier_name, 'deployable_name': deployable_name, 'tenant_name': tenant_name})
        if not tenant:
            raise RuntimeError(
                "Tenant '{}' not found for tier '{}' and deployable '{}'".format(tenant_name, tier_name, deployable_name)
            )
    else:
        tenant = None

    if tenant:
        tenant_name = tenants.get_foreign_row(tenant, 'tenant-names')[0]
        product = ts.get_table('tenant-names').get_foreign_row(tenant_name, 'products')[0]
        organization = ts.get_table('products').get_foreign_row(product, 'organizations')[0]
    else:
        tenant_name = None
        product = None
        organization = None

    conf = conf_tuple(
        table_store=ts,
        tenant=tenant,
        tier=ts.get_table('tiers').get({'tier_name': tier_name}),
        deployable=ts.get_table('deployables').get({'deployable_name': deployable_name, 'tier_name': tier_name}),
        domain=ts.get_table('domain'),
        tenant_name=tenant_name,
        tenants=ts.get_table('tenants').find({'tier_name': tier_name, 'deployable_name': deployable_name, 'state': 'active'}),
        product=product,
        organization=organization,
        drift_app=drift_app,
    )

    return conf


def diff_table_stores(ts1, ts2):

    report = {}

    try:
        from jsondiff import diff
    except ImportError as e:
        diff = None
        print "Can't import jsondiff' library:", e
        print "To get diffs, run this: pip install jsondiff"

    # Just to keep things fresh, refresh both table stores
    ts1.refresh_metadata()
    ts2.refresh_metadata()

    def timediff_is_older(t1, t2):
        """Returns the time diff sans secs, and if 't1' is older than 't2'."""
        t1 = datetime.strptime(t1, '%Y-%m-%dT%H:%M:%S.%fZ')
        t2 = datetime.strptime(t2, '%Y-%m-%dT%H:%M:%S.%fZ')
        if t1 < t2:
            return str(t2 - t1).split('.', 1)[0], True
        else:
            return str(t1 - t2).split('.', 1)[0], False

    if ts1.meta['last_modified'] == ts2.meta['last_modified']:
        report['last_modified'] = ts1.meta['last_modified']
    else:
        td, is_older = timediff_is_older(ts1.meta['last_modified'], ts2.meta['last_modified'])
        report['last_modified_diff'] = td, is_older, ts1.meta['last_modified'], ts2.meta['last_modified']

    report['tables'] = {}

    for table_name in ts1.tables:
        table_diff = {}
        report['tables'][table_name] = table_diff

        try:
            t1, t2 = ts1.get_table(table_name), ts2.get_table(table_name)
        except KeyError as e:
            print "Can't compare table '{}' as it's missing from origin.".format(table_name)
            continue

        t1_meta, t2_meta = ts1.get_table_metadata(table_name), ts2.get_table_metadata(table_name)
        is_older = False
        if t1_meta['last_modified'] != t2_meta['last_modified']:
            td, is_older = timediff_is_older(t1_meta['last_modified'], t2_meta['last_modified'])
            table_diff['last_modified'] = td, is_older, t1_meta['last_modified'], t2_meta['last_modified']

        if t1_meta['md5'] != t2_meta['md5']:
            diffdump = None
            if diff:
                if is_older:
                    diffdump = diff(t1.find(), t2.find(), syntax='symmetric', marshal=True)
                else:
                    diffdump = diff(t2.find(), t1.find(), syntax='symmetric', marshal=True)

            table_diff['md5'] = diffdump, is_older, t1_meta['md5'], t2_meta['md5']

    return report

