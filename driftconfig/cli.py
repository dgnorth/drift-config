# -*- coding: utf-8 -*-
import os
import os.path
import sys
from datetime import datetime, timedelta
import time
import json
import logging
import subprocess
import textwrap
import collections

import click
from click import echo, secho
import six
from six.moves import input

# pygments is optional for now, used for syntax highlighting
try:
    got_pygments = True
    from pygments import highlight
    from pygments.lexers import get_lexer_by_name
    from pygments.formatters import get_formatter_by_name
except ImportError:
    got_pygments = False

from driftconfig.relib import create_backend, get_store_from_url, diff_meta, diff_tables, CHECK_INTEGRITY, copy_table_store
from driftconfig.config import get_drift_table_store, get_redis_cache_backend, push_to_origin, pull_from_origin, TSTransaction, TSLocal
from driftconfig.config import update_cache
from driftconfig.backends import FileBackend
from driftconfig.util import (
    config_dir, get_domains, get_default_drift_config, get_default_drift_config_and_source,
    define_tenant, prepare_tenant_name, provision_tenant_resources,
    get_tier_resource_modules, register_tier_defaults, register_this_deployable_on_tier,
    register_this_deployable, register_this_deployable_for_product
)
from driftconfig import testhelpers

log = logging.getLogger(__name__)


# Enable simple in-line color and styling of output
try:
    from colorama.ansi import Fore, Back, Style
    styles = {'f': Fore, 'b': Back, 's': Style}
    # Example: "{s.BRIGHT}Bold and {f.RED}red{f.RESET}{s.NORMAL}".format(**styles)
except ImportError:
    class EmptyString(object):
        def __getattr__(self, name):
            return ''

    styles = {'f': EmptyString(), 'b': EmptyString(), 's': EmptyString()}


def get_options(parser):

    subparsers = parser.add_subparsers(
        title="Config file management",
        description="These sets of commands help you with setting up configuration for Drift products.",
        dest="command",
    )

    # 'init' command
    p = subparsers.add_parser(
        'init',
        help='Initialize configuration from a given source.',
        description="Initialize configuration using a given source, like S3, and write it somewhere else, like on local disk.\n"
                    "An example of S3 source: s3://bucket-name/path-name"
    )
    p.add_argument(
        'source',
        action='store',
    )
    p.add_argument(
        '--ignore-errors', '-i',
        action='store_true',
        help='Ignore any errors.'
    )

    # 'list' command
    p = subparsers.add_parser(
        'list',
        help='List locally stored configurations.',
        description="List out all configuration that are stored locally."
    )

    # 'pull' command
    p = subparsers.add_parser(
        'pull',
        help='Pull config.',
        description="Pull latest configuration from source."
    )
    p.add_argument(
        '--loop',
        action='store_true',
        help='pull config continuously for 1 minute.'
    )
    p.add_argument(
        '--ignore-if-modified', '-i',
        action='store_true',
        help='Force a pull from origin even though local version has been modified.'
    )
    p.add_argument(
        '-f', '--force',
        action='store_true',
        help='Force a pull from origin even though local version matches.'
    )
    p.add_argument(
        'domain',
        action='store', nargs='?',
    )

    # 'cache' command
    p = subparsers.add_parser(
        'cache',
        help='Update cache for config.',
        description="Add the config to Redis cache ."
    )
    p.add_argument(
        'domain',
        action='store', nargs='?',
    )
    p.add_argument(
        '-t', '--tier',
        help="The tier on which to update cache, or all if ommitted."
    )

    # 'migrate' command
    p = subparsers.add_parser(
        'migrate',
        help='Migrate config.',
        description="Migrate config to latest definition of TableStore."
    )
    p.add_argument(
        'domain',
        action='store',
    )

    # 'push' command
    p = subparsers.add_parser(
        'push',
        help='Push config.',
        description="Push local config to source. Use with causion."
    )
    p.add_argument(
        'domain',
        action='store',
    )
    p.add_argument(
        '-f', '--force',
        action='store_true',
        help='Force a push to origin even though origin has changed.'
    )

    # 'copy' command
    p = subparsers.add_parser(
        'copy',
        help='Copy config.',
        description="Copy config from one url to another."
    )
    p.add_argument(
        'source_url',
        action='store', help="Source url, or . for default config url."
    )
    p.add_argument(
        'dest_url',
        action='store',
    )
    p.add_argument(
        '-p', '--pickle',
        action='store_true', help="Use pickle format for destination."
    )

    # 'diff' command
    p = subparsers.add_parser(
        'diff',
        help='Diff origin.',
        description="Diff local config to origin."
    )
    p.add_argument(
        'domain',
        action='store', help="Short name to identify the domain or owner of the config.",
    )
    p.add_argument(
        '-d', '--details',
        action='store_true',
        help='Do a detailed diff on modified tables.'
    )

    # MIGRATED FROM drift-admin register command
    p = subparsers.add_parser(
        'register',
        help='Register Drift deployable.',
        description=""
    )
    p.add_argument(
        'project-dir',
        action='store',
        help="Path to project root directory. Default is current working directory.",
        nargs='?',
    )
    p.add_argument(
        "--preview", help="Only preview the changes, do not commit to origin.", action="store_true"
    )

    # MIGRATED FROM drift-admin tenant command suite
    # The create command
    p = subparsers.add_parser(
        'create-tenant',
        help="Create a new tenant for a given product.",
        description="Create a new tenant for a given product."
    )
    p.add_argument(
        'tenant-name',
        action='store',
        help="Name of the tenant.",
    )
    p.add_argument(
        'product-name',
        action='store',
        help="Name of the product.",
    )
    p.add_argument(
        'tier-name',
        action='store',
        help="Name of the tier.",
    )
    p.add_argument(
        '--config',
        help="Specify which config source to use. Will override 'DRIFT_CONFIG_URL' environment variable."
    )
    p.add_argument(
        "--preview", help="Only preview the changes, do not commit to origin.", action="store_true"
    )

    # The refresh command
    p = subparsers.add_parser(
        'refresh-tenant',
        help="Refresh tenant.",
        description="Refresh a tenants on a tier."
    )
    p.add_argument(
        'tenant-name',
        action='store',
        help="Name of the tenant.",
        nargs='?',
    )
    p.add_argument(
        '--config',
        help="Specify which config source to use. Will override 'DRIFT_CONFIG_URL' environment variable."
    )
    p.add_argument(
        "--preview", help="Only preview the changes, do not commit to origin.", action="store_true"
    )

    # The provision command
    p = subparsers.add_parser(
        'provision-tenant',
        help="Provision tenant.",
        description="Provision and prepare resources for a tenant."
    )
    p.add_argument(
        'tenant-name',
        action='store',
        help="Name of the tenant.",
    )
    p.add_argument(
        'deployable-name',
        action='store',
        help="Name of the deployable. Specify 'all' to include all deployables.",
    )
    p.add_argument(
        '--config',
        help="Specify which config source to use. Will override 'DRIFT_CONFIG_URL' environment variable."
    )
    p.add_argument(
        "--preview", help="Only preview the changes, do not commit to origin.", action="store_true"
    )

    # The assign-tier command
    p = subparsers.add_parser(
        'assign-tier',
        help="Assign a deployable to a tier.",
    )
    p.add_argument(
        'deployable-name',
        action='store',
        help="Name of the deployable.",
    )
    p.add_argument(
        "--tiers", help="List of tiers to enable the deployable, or all tiers if omitted.",
        nargs='*',
    )
    p.add_argument(
        "--inactive", help="Mark the deployable inactive. By default the deployable will be marked as active.", action="store_true"
    )
    p.add_argument(
        '--config',
        help="Specify which config source to use. Will override 'DRIFT_CONFIG_URL' environment variable."
    )
    p.add_argument(
        "--preview", help="Only preview the changes, do not commit to origin.", action="store_true"
    )

    # The assign-product command
    p = subparsers.add_parser(
        'assign-product',
        help="Assign a deployable to a product.",
    )
    p.add_argument(
        'deployable-name',
        action='store',
        help="Name of the deployable.",
    )
    p.add_argument(
        "--products", help="List of products for which to enable the deployable.",
        nargs='*',
    )
    p.add_argument(
        '--config',
        help="Specify which config source to use. Will override 'DRIFT_CONFIG_URL' environment variable."
    )
    p.add_argument(
        "--preview", help="Only preview the changes, do not commit to origin.", action="store_true"
    )

    # CRUD command suite -------------------------------------------
    # 'create' command
    p = subparsers.add_parser(
        'create',
        description='Create a new Drift configuration.',
    )
    p.add_argument(
        'domain',
        action='store', help="Short name to identify the domain or owner of the config.",
    )
    p.add_argument(
        'source',
        action='store', help="The source location of the config, normally an S3 location."
    )
    p.add_argument(
        '--display-name',
        action='store', help="Display name."
    )


def init_command(args):
    echo("Initializing config from {}".format(args.source))
    if args.ignore_errors:
        del CHECK_INTEGRITY[:]
    ts = create_backend(args.source).load_table_store()
    domain_name = ts.get_table('domain')['domain_name']
    echo("Config domain name: {}".format(domain_name))
    local_store = create_backend('file://' + config_dir(domain_name, user_dir=args.user_dir))
    local_store.save_table_store(ts)
    echo("Config stored at: {}".format(local_store))


def _format_domain_info(domain_info):
    domain = domain_info['table_store'].get_table('domain')
    return "{}: \"{}\" at '{}'. Origin: '{}'".format(
        domain['domain_name'], domain['display_name'], domain_info['path'], domain['origin'])


def list_command(args):
    # Enumerate subfolders at drift/config and see what's there
    domains = get_domains(user_dir=args.user_dir)
    if not domains:
        echo("No Drift configuration found at " + config_dir('', user_dir=args.user_dir))
    else:
        for d in domains.values():
            echo(_format_domain_info(d))


def pull_command(args):
    if args.loop:
        pull_config_loop(args)
    else:
        _pull_command(args)


def pull_config_loop(args):
    # TODO replace with click progress bar
    echo("Starting the pull config loop")
    while now() < end_time:
        st = time.time()
        _pull_command(args)
        diff = time.time() - st
        this_sleep_time = max(sleep_time - diff, 0)
        echo("Waiting for %.1f sec" % this_sleep_time)
        time.sleep(this_sleep_time)
    echo("Completed in %.1f sec" % (now() - start_time).total_seconds())


def _pull_command(args):
    for domain_name, domain_info in get_domains(user_dir=args.user_dir).items():
        if args.domain and args.domain != domain_name:
            continue

        result = pull_from_origin(domain_info['table_store'], ignore_if_modified=args.ignore_if_modified, force=args.force)

        if not result['pulled']:
            echo("Pull failed for " + domain_name + ". Reason: " + result['reason'])
            if result['reason'] == 'local_is_modified':
                echo("Use --ignore-if-modified to overwrite local changes.")
            else:
                echo("Use --force to force a pull.")
        else:
            if result['reason'] == 'pulled_from_origin':
                local_backend = create_backend('file://' + domain_info['path'])
                local_backend.save_table_store(result['table_store'])

            echo("Config for {} pulled. Reason: {}".format(domain_name, result['reason']))


def cache_command(args):
    if args.domain:
        os.environ['DRIFT_CONFIG_URL'] = args.domain
    ts = get_default_drift_config()
    echo("Updating cache for '{}' - {}".format(
        ts.get_table('domain')['domain_name'], ts))

    for tier in ts.get_table('tiers').find():
        tier_name = tier['tier_name']
        if args.tier and args.tier.upper() != tier_name:
            continue
        secho("{}: ".format(tier_name), nl=False, bold=True)
        try:
            b = update_cache(ts, tier_name)
        except Exception as e:
            if "Timeout" not in str(e):
                raise
            secho("Updating failed. VPN down? {}".format(e), fg='red', bold=True)
        else:
            if b:
                secho("Cache updated. Url: {}".format(b.get_url()))
            else:
                secho("No Redis resource defined for this tier.", fg='red', bold=True)

    '''
    # bench test:
    def test_redis_config_fetch(count=10):
        import time
        import os
        from driftconfig.util import get_default_drift_config
        os.environ['DRIFT_CONFIG_URL'] = 'redis://redis.devnorth.dg-api.com/?prefix=dgnorth'
        t = time.time()
        for i in range(count):
            ts = get_default_drift_config()
        t = time.time() - t
        avg = t / count
        echo("Average time to fetch config from redis: %.1f ms." % (avg * 1000.0))
    '''


def migrate_command(args):
    echo("Migrating '{}'".format(args.domain))
    path = config_dir(args.domain, user_dir=args.user_dir)
    if not os.path.exists(path):
        echo("Path not found: " + path)
        sys.exit(1)

    ts = get_drift_table_store()

    class PatchBackend(FileBackend):
        def load_data(self, file_name):
            path_name = self.get_filename(file_name)
            if not os.path.exists(path_name):
                # Attempt to create it just-in-time as a table with zero rows
                head, tail = os.path.split(path_name)
                if not os.path.exists(head):
                    os.makedirs(head)
                with open(path_name, 'w') as f:
                    f.write('[]\n')
            return super(PatchBackend, self).load_data(file_name)

    local_store = PatchBackend(path)
    ts._load_from_backend(local_store, skip_definition=True)
    local_store.save_table_store(ts)


def now():
    return datetime.utcnow()


sleep_time = 10
run_time = 50

start_time = now()
end_time = start_time + timedelta(seconds=run_time)


def push_command(args):
    domain_info = get_domains(user_dir=args.user_dir).get(args.domain)
    if not domain_info:
        echo("Can't push '{}'.".format(args.domain))
        sys.exit(1)

    ts = domain_info['table_store']
    origin = ts.get_table('domain')['origin']
    echo("Pushing local config to source " + origin)
    result = push_to_origin(ts, args.force)
    if not result['pushed']:
        echo("Push failed. Reason: " + result['reason'])
        echo("Origin has changed. Use --force to force push.")
        if 'time_diff' in result:
            echo("Time diff " + result['time_diff'])
    else:
        echo("Config pushed. Reason: " + result['reason'])
        local_store = create_backend('file://' + domain_info['path'])
        local_store.save_table_store(ts)


def copy_command(args):
    echo("Copy '%s' to '%s'" % (args.source_url, args.dest_url))
    if args.source_url == '.':
        ts = get_default_drift_config()
    else:
        ts = get_store_from_url(args.source_url)
    b = create_backend(args.dest_url)
    b.default_format = 'pickle' if args.pickle else 'json'
    b.save_table_store(ts)
    echo("Done.")


def create_command(args):

    domain_info = get_domains(user_dir=args.user_dir).get(args.domain)
    if domain_info:
        echo("The domain name '{}' is taken:".format(args.domain))
        echo(_format_domain_info(domain_info))
        sys.exit(1)

    # Force s3 naming convention. The root folder name and domain name must match.
    if args.source.startswith('s3://'):
        # Strip trailing slashes
        if args.source.endswith('/'):
            args.source = args.source[:-1]

        s3_backend = create_backend(args.source)
        target_folder = s3_backend.folder_name.rsplit('/')[-1]
        if target_folder != args.domain:
            echo("Error: For S3 source, the target folder name and domain name must match.")
            echo("Target folder is '{}' but domain name is '{}'".format(target_folder, args.domain))
            sys.exit(1)
    elif args.source.startswith('file://'):
        # Expand user vars
        args.source = args.source.replace('~', os.path.expanduser('~'))

    # Get empty table store for Drift.
    ts = get_drift_table_store()
    ts.get_table('domain').add(
        {'domain_name': args.domain, 'origin': args.source, 'display_name': args.display_name or ''})

    # Save it locally
    domain_folder = config_dir(args.domain, user_dir=args.user_dir)
    local_store = create_backend('file://' + domain_folder)
    local_store.save_table_store(ts)
    echo("New config for '{}' saved to {}.".format(args.domain, domain_folder))
    echo("Pushing to origin...")
    result = push_to_origin(ts, _first=True)
    if not result['pushed']:
        echo("Push failed. Reason: " + result['reason'])
    echo("Done.")


_ENTRY_TO_TABLE_NAME = {
    'tier': 'tiers',
    'deployable': 'deployable-names',
    'organization': 'organizations',
    'product': 'products',
}


def diff_command(args):
    # Get local table store and its meta state
    domain_info = get_domains(user_dir=args.user_dir).get(args.domain)
    if domain_info is None:
        secho("Configuration not found: {}".format(args.domain), fg='red')
        sys.exit(1)
    local_ts = domain_info['table_store']
    local_m1, local_m2 = local_ts.refresh_metadata()

    # Get origin table store meta info
    origin = local_ts.get_table('domain')['origin']
    origin_backend = create_backend(origin)
    origin_ts = origin_backend.load_table_store()
    origin_meta = origin_ts.meta.get()

    local_diff = ("Local store and scratch", local_m1, local_m2, False)
    origin_diff = ("Local and origin", origin_meta, local_m2, args.details)

    for title, m1, m2, details in local_diff, origin_diff:
        diff = diff_meta(m1, m2)
        if diff['identical']:
            echo(title + " is clean.")
        else:
            echo(title + " are different:")
            echo("\tFirst checksum:  " + diff['checksum']['first'][:7])
            echo("\tSecond checksum: " + diff['checksum']['second'][:7])
            if diff['modified_diff']:
                echo("\tTime since pull: " + str(diff['modified_diff']).split('.')[0])

            echo("\tNew tables: " + str(diff['new_tables']))
            echo("\tDeleted tables: " + str(diff['deleted_tables']))
            echo("\tModified tables: " + str(diff['modified_tables']))

            if details:
                # Diff origin
                origin_ts = get_store_from_url(origin)
                for table_name in diff['modified_tables']:
                    t1 = local_ts.get_table(table_name)
                    t2 = origin_ts.get_table(table_name)
                    tablediff = diff_tables(t1, t2)
                    echo("\nTable diff for " + table_name + "\n(first=local, second=origin):")
                    echo(json.dumps(tablediff, indent=4, sort_keys=True))


def register_command(args):
    project_dir = vars(args)['project-dir'] or '.'
    project_dir = os.path.abspath(project_dir)
    register_deployable(project_dir, args.preview)


def _get_package_info(project_dir):
    """
    Returns info from current package.
    """

    _package_classifiers = [
        'name',
        'version',
        'description',
        'long-description',
        'author',
        'author-email',
        'license'
    ]
    classifier = None
    cmd = [sys.executable, 'setup.py'] + ['--' + c for c in _package_classifiers]
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        cwd=project_dir
    )
    out, err = p.communicate()
    if p.returncode != 0:
        if six.PY3 and err:
            err = err.decode("utf-8")
        raise RuntimeError(
            "Can't get '{}' of this deployable. Error: {} - {}".format(classifier, p.returncode, err)
        )

    info = dict(zip(_package_classifiers, out.decode().splitlines()))
    return info


def register_deployable(project_dir=None, preview=False):

    project_dir = project_dir or '.'
    echo("Project Directory: " + project_dir)

    info = _get_package_info(project_dir)
    name = info['name']

    echo("Registering/updating deployable {}:".format(name))
    echo("Package info:")
    echo(pretty(info))
    echo("")

    # TODO: This is perhaps not ideal, or what?
    config_filename = os.path.join(project_dir, 'config', 'config.json')
    config_filename = os.path.expanduser(config_filename)
    log.info("Loading configuration from %s", config_filename)
    with open(config_filename) as f:
        app_config = json.load(f)

    # Make project_dir importable.
    sys.path.insert(0, project_dir)

    with TSTransaction(commit_to_origin=not preview) as ts:

        ret = register_this_deployable(
            ts=ts,
            package_info=info,
            resources=app_config.get("resources", []),
            resource_attributes=app_config.get("resource_attributes", {}),
        )

        orig_row = ret['old_registration']
        row = ret['new_registration']

        if orig_row is None:
            echo("New registration entry added:")
            echo(pretty(row))
        elif orig_row == row:
            echo("Current registration unchanged:")
            echo(pretty(row))
        else:
            echo("Updating current registration info:")
            echo(pretty(row))
            echo("\nPrevious registration info:")
            echo(pretty(orig_row))

    if preview:
        echo("Preview changes only, not committing to origin.")


def create_tenant_command(args):

    tier_name = vars(args)['tier-name']
    if args.config:
        os.environ['DRIFT_CONFIG_URL'] = args.config

    with TSTransaction(commit_to_origin=not args.preview) as ts:
        prep = prepare_tenant_name(
            ts=ts,
            tenant_name=vars(args)['tenant-name'],
            product_name=vars(args)['product-name']
        )
        tenant_name = prep['tenant_name']

        tenant = ts.get_table('tenant-names').get({'tenant_name': tenant_name})
        if tenant:
            if tenant['tier_name'] != tier_name:
                echo("Tenant '{}' is on tier '{}'. Exiting.".format(tenant_name, tenant['tier_name']))
                sys.exit(1)

            echo("Tenant '{}' already exists. Refreshing it for tier '{}'...".format(
                tenant_name, tier_name))

        result = define_tenant(
            ts=ts,
            tenant_name=tenant_name,
            product_name=prep['product']['product_name'],
            tier_name=tier_name
        )

    echo("Tenant '{}' created/refreshed on tier '{}'.".format(tenant_name, tier_name))
    echo(pretty(result))
    if args.preview:
        echo("\nPreview changes only, not committing to origin.")
        sys.exit(0)


def refresh_tenant_command(args):

    tenant_name = vars(args)['tenant-name']
    echo("Refreshing '{}':".format(tenant_name))
    if args.config:
        os.environ['DRIFT_CONFIG_URL'] = args.config

    # Out of convenience, add current dir to sys.path so the local project can
    # be found during imports.
    sys.path.insert(0, '.')

    with TSTransaction(commit_to_origin=not args.preview) as ts:
        tenant_info = ts.get_table('tenant-names').get({'tenant_name': tenant_name})
        if not tenant_info:
            echo("Tenant '{}' not found!".format(tenant_name))
            sys.exit(1)

        result = define_tenant(
            ts=ts,
            tenant_name=tenant_name,
            product_name=tenant_info['product_name'],
            tier_name=tenant_info['tier_name'],
        )

    echo("Result:")
    echo(pretty(result))
    if args.preview:
        echo("\nPreview changes only, not committing to origin.")
        sys.exit(0)


def provision_tenant_command(args):

    tenant_name = vars(args)['tenant-name']
    deployable_name = vars(args)['deployable-name']

    echo("Provisioning '{}' for {}:".format(tenant_name, deployable_name))
    if deployable_name == 'all':
        deployable_name = None

    if args.config:
        os.environ['DRIFT_CONFIG_URL'] = args.config

    # Out of convenience, add current dir to sys.path so the local project can
    # be found during imports.
    sys.path.insert(0, '.')

    with TSTransaction(commit_to_origin=not args.preview) as ts:
        tenant_info = ts.get_table('tenant-names').get({'tenant_name': tenant_name})
        if not tenant_info:
            echo("Tenant '{}' not found!".format(tenant_name))
            sys.exit(1)

        # Refresh for good measure
        define_tenant(
            ts=ts,
            tenant_name=tenant_name,
            product_name=tenant_info['product_name'],
            tier_name=tenant_info['tier_name'],
        )

        report = provision_tenant_resources(
            ts=ts,
            tenant_name=tenant_name,
            deployable_name=deployable_name,
            preview=args.preview
        )

    echo("Result:")
    echo(pretty(report))
    if args.preview:
        echo("\nPreview changes only, not committing to origin.")
        sys.exit(0)


def assign_tier_command(args):
    deployable_name = vars(args)['deployable-name']
    echo("Assigning '{}':".format(deployable_name))

    if args.config:
        os.environ['DRIFT_CONFIG_URL'] = args.config

    # Out of convenience, add current dir to sys.path so the local project can
    # be found during imports.
    sys.path.insert(0, '.')

    is_active = not args.inactive

    with TSTransaction(commit_to_origin=not args.preview) as ts:
        old_ts = copy_table_store(ts)

        names = [d['deployable_name'] for d in ts.get_table('deployable-names').find()]
        if not names:
            echo("No deployable registered. See 'drift-admin register' for more info.")
            sys.exit(1)

        if deployable_name not in names:
            echo("Deployable '{}' not found. Select one of: {}.".format(
                deployable_name,
                ', '.join(names)
            ))
            sys.exit(1)

        if not args.tiers:
            args.tiers = [tier['tier_name'] for tier in ts.get_table('tiers').find()]

        for tier_name in args.tiers:
            echo("Enable deployable on tier {s.BRIGHT}{}{s.NORMAL}:".format(tier_name, **styles))
            tier = ts.get_table('tiers').get({'tier_name': tier_name})
            if not tier:
                echo("{f.RED}Tier '{}' not found! Exiting.".format(tier_name, **styles))
                sys.exit(1)

            ret = register_this_deployable_on_tier(
                ts, tier_name=tier_name, deployable_name=deployable_name)

            if ret['new_registration']['is_active'] != is_active:
                ret['new_registration']['is_active'] = is_active
                echo("Note: Marking this deployable as {} on tier '{}'.".format(
                    "active" if is_active else "inactive", tier_name))

            # For convenience, register resource default values as well. This
            # is idempotent so it's fine to call it periodically.
            resources = get_tier_resource_modules(
                ts=ts, tier_name=tier_name, skip_loading=False, ignore_import_errors=True)

            # See if there is any attribute that needs prompting,
            # Any default parameter from a resource module that is marked as <PLEASE FILL IN> and
            # is not already set in the config, is subject to prompting.
            tier = ts.get_table('tiers').get({'tier_name': tier_name})
            config_resources = tier.get('resources', {})

            for resource in resources:
                for k, v in resource['default_attributes'].items():
                    if v == "<PLEASE FILL IN>":
                        # Let's prompt if and only if the value isn't already set.
                        attributes = config_resources.get(resource['module_name'], {})
                        if k not in attributes or attributes[k] == "<PLEASE FILL IN>":
                            echo("Enter value for {s.BRIGHT}{}.{}{s.NORMAL}: ".format(
                                resource['module_name'], k, **styles), nl=False)
                            resource['default_attributes'][k] = input()

            echo("\nDefault values for resources configured for this tier:")
            echo(pretty(config_resources))

            register_tier_defaults(ts=ts, tier_name=tier_name, resources=resources)

            echo("\nRegistration values for this deployable on this tier:")
            echo(pretty(ret['new_registration']))
            echo("")

        # Display the diff
        _diff_ts(ts, old_ts)

    if args.preview:
        echo("Preview changes only, not committing to origin.")


def assign_product_command(args):
    deployable_name = vars(args)['deployable-name']
    echo("Assigning '{}':".format(deployable_name))

    if args.config:
        os.environ['DRIFT_CONFIG_URL'] = args.config

    # Out of convenience, add current dir to sys.path so the local project can
    # be found during imports.
    sys.path.insert(0, '.')

    with TSTransaction(commit_to_origin=not args.preview) as ts:
        old_ts = copy_table_store(ts)

        names = [d['deployable_name'] for d in ts.get_table('deployable-names').find()]
        if not names:
            echo("No deployable registered. See 'drift-admin register' for more info.")
            sys.exit(1)

        if deployable_name not in names:
            echo("Deployable '{}' not found. Select one of: {}.".format(
                deployable_name,
                ', '.join(names)
            ))
            sys.exit(1)

        if not args.products:
            echo("No product selected. Select one of: {}".format(
                ', '.join([product['product_name'] for product in ts.get_table('products').find()])
            ))

        for product_name in args.products:
            echo("Enable deployable for product {s.BRIGHT}{}{s.NORMAL}:".format(product_name, **styles))
            product = ts.get_table('products').get({'product_name': product_name})
            if not product:
                echo("{f.RED}Product '{}' not found! Exiting.".format(product_name, **styles))
                sys.exit(1)

            ret = register_this_deployable_for_product(
                ts, product_name=product_name, deployable_name=deployable_name)

            # # For convenience, register resource default values as well. This
            # # is idempotent so it's fine to call it periodically.
            # resources = get_tier_resource_modules(
            #     ts=ts, tier_name=product_name, skip_loading=False, ignore_import_errors=True)
            #
            # # See if there is any attribute that needs prompting,
            # # Any default parameter from a resource module that is marked as <PLEASE FILL IN> and
            # # is not already set in the config, is subject to prompting.
            # product = ts.get_table('tiers').get({'tier_name': product_name})
            # config_resources = product.get('resources', {})
            #
            # for resource in resources:
            #     for k, v in resource['default_attributes'].items():
            #         if v == "<PLEASE FILL IN>":
            #             # Let's prompt if and only if the value isn't already set.
            #             attributes = config_resources.get(resource['module_name'], {})
            #             if k not in attributes or attributes[k] == "<PLEASE FILL IN>":
            #                 echo("Enter value for {s.BRIGHT}{}.{}{s.NORMAL}: ".format(
            #                     resource['module_name'], k, **styles), nl=False)
            #                 resource['default_attributes'][k] = input()
            #
            # echo("\nDefault values for resources configured for this tier:")
            # echo(pretty(config_resources))
            #
            # register_tier_defaults(ts=ts, tier_name=product_name, resources=resources)
            #
            # echo("\nRegistration values for this deployable on this tier:")
            # echo(pretty(ret['new_registration']))
            # echo("")

        # Display the diff
        _diff_ts(ts, old_ts)

    if args.preview:
        echo("Preview changes only, not committing to origin.")


def _diff_ts(ts1, ts2):
    from driftconfig.relib import diff_meta, diff_tables
    # Get local table store and its meta state

    ts1 = copy_table_store(ts1)
    ts2 = copy_table_store(ts2)
    local_m1, local_m2 = ts1.refresh_metadata()

    # Get origin table store meta info
    origin_meta = ts2.meta.get()

    title = "Local and origin"
    m1, m2 = origin_meta, local_m2
    diff = diff_meta(m1, m2)

    if diff['identical']:
        echo(title + " is clean.")
    else:
        echo(title + " are different:")
        echo("\tFirst checksum:  " + diff['checksum']['first'][:7])
        echo("\tSecond checksum: " + diff['checksum']['second'][:7])
        if diff['modified_diff']:
            echo("\tTime since pull: " + str(diff['modified_diff']).split('.')[0])

        echo("\tNew tables: {}".format(diff['new_tables']))
        echo("\tDeleted tables: {}".format(diff['deleted_tables']))
        echo("\tModified tables: {}".format(diff['modified_tables']))

        try:
            import jsondiff
        except ImportError:
            echo("To get detailed diff do {s.BRIGHT}pip install jsondiff{s.NORMAL}".format(**styles))
        else:
            # Diff origin
            for table_name in diff['modified_tables']:
                t1 = ts1.get_table(table_name)
                t2 = ts2.get_table(table_name)
                tablediff = diff_tables(t1, t2)
                echo("\nTable diff for {s.BRIGHT}{}{s.NORMAL}".format(table_name, **styles))

                for modified_row in tablediff['modified_rows']:
                    d = json.loads(jsondiff.diff(
                        modified_row['second'], modified_row['first'], dump=True)
                    )
                    echo(pretty(d))


def run_command(args):
    fn = globals()["{}_command".format(args.command.replace("-", "_"))]
    fn(args)


def main(as_module=False):
    import argparse
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--loglevel', default='WARNING')
    parser.add_argument('--nocheck', action='store_true', help="Skip all relational integrity and schema checks.")
    parser.add_argument('--user-dir', action='store_true', help="Choose user directory over site for locally stored configs.")
    get_options(parser)
    args = parser.parse_args()

    if args.loglevel:
        logging.basicConfig(level=args.loglevel)

    if args.nocheck:
        import driftconfig.relib
        del driftconfig.relib.CHECK_INTEGRITY[:]

    if not args.command:
        parser.print_usage()
        sys.exit(1)
    fn = globals()["{}_command".format(args.command.replace("-", "_"))]
    fn(args)


if __name__ == '__main__':
    main(as_module=True)


def _header(ts):
    domain = ts.get_table('domain')
    secho("Drift config DB ", nl=False)
    secho(domain['domain_name'], bold=True, nl=False)
    secho(" at origin ", nl=False)
    secho(domain['origin'], bold=True)


def _epilogue(ts):
    name = ts.get_table('domain')['domain_name']
    secho("Run \"driftconfig diff {} -d\" to see changes. Run \"driftconfig push {}\" to commit them.".format(name, name))


class Globals(object):
    pass


pass_repo = click.make_pass_decorator(Globals)


@click.group(context_settings={'help_option_names': ['-h', '--help']})
@click.option(
    '--config-url', '-u', envvar='DRIFT_CONFIG_URL', metavar='',
    help="Url to DB origin.")
@click.option(
    '--verbose', '-v', is_flag=True,
    help='Enables verbose mode.')
@click.option(
    '--organization', '-o', is_flag=True,
    help='Specify organization name/short name.')
@click.option(
    '--product', '-p', is_flag=True,
    help='Specify product name.')
@click.version_option('1.0')
@click.pass_context
def cli(ctx, config_url, verbose, organization, product):
    """This command line tool helps you manage and maintain Drift
    Configuration databases.
    """
    ctx.obj = Globals()
    ctx.obj.config_url = config_url
    if config_url:
        os.environ['DRIFT_CONFIG_URL'] = config_url
    ctx.obj.verbose = verbose
    ctx.obj.organization = organization
    ctx.obj.product = product


@cli.group()
def list():
    """List out information in a configuration DB."""


@list.command()
def configs():
    """List out all Drift configuration DB's that are active on this machine."""
    domains = get_domains()
    if not domains:
        secho(
            "No Drift configuration found on this machine. Run 'init' or 'create' command "
            "to remedy.")
    else:
        got_default = False

        for domain_info in domains.values():
            ts = domain_info['table_store']
            domain = ts.get_table('domain')
            is_default = domain['domain_name'] == ts.get_table('domain')['domain_name']
            if is_default:
                secho(domain['domain_name'] + " [DEFAULT]:", bold=True, nl=False)
                got_default = True
            else:
                secho(domain['domain_name'] + ":", bold=True, nl=False)

            secho(" \"{}\"".format(domain['display_name']), fg='green')
            secho("\tOrigin: " + domain['origin'])
            secho("\tLocal: " + domain_info['path'])
            for tier in ts.get_table('tiers').find():
                b = get_redis_cache_backend(ts, tier['tier_name'])
                if b:
                    secho("\tCache [{}]: {}".format(tier['tier_name'], b.get_url()))
            secho("")

        if got_default:
            if 'DRIFT_CONFIG_URL' in os.environ:
                secho("The default config is specified using the 'DRIFT_CONFIG_URL' environment variable.")
            else:
                secho("The config above is the default one as it's the only one cached locally in ~/.drift/config.")
        else:
            secho("Note: There is no default config specified!")


@list.command()
def deployables():
    """Display registration info for deployables."""
    ts = get_default_drift_config()
    echo("List of Drift deployable plugins in ", nl=False)
    _header(ts)
    deployables = ts.get_table('deployable-names')

    secho("Deployables and api routes:\n", bold=True)

    def join_tables(master_table, tables, search_criteria=None, cb=None):
        """
        Joins rows from 'tables' to the rows of 'master_table' and returns them
        as a single sequence.
        'search_criteria' is applied to the 'master_table'.
        """
        result = []
        rows = master_table.find(search_criteria or {})
        for row in rows:
            row = row.copy()
            for table in tables:
                other = table.get(row)
                if other:
                    row.update(other)
            if cb:
                cb(row)
            result.append(row)
        return result

    def cb(row):
        """Generate list of tier names using info from 'deployables' table."""
        crit = {'deployable_name': row['deployable_name'], 'is_active': True}
        tiers = [d['tier_name'] for d in ts.get_table('deployables').find(crit)]
        row['tiers'] = ', '.join(sorted(tiers))

    head = ['deployable_name', 'api', 'requires_api_key', 'tiers', 'display_name']
    rows = join_tables(
        master_table=deployables,
        tables=[ts.get_table('routing'), ts.get_table('deployable-names')],
        cb=cb,
    )
    tabulate(head, rows, indent='  ')


@list.command()
@click.option('-name', '-n', 'tenant_name', type=str, help="Show full info for given tenant.")
def tenants(tenant_name):
    """Display tenant info."""
    conf = get_default_drift_config()
    _header(conf)

    if tenant_name is None:
        tabulate(
            ['organization_name', 'product_name', 'tier_name', 'tenant_name', 'reserved_at', 'reserved_by'],
            conf.get_table('tenant-names').find(),
            indent='  ',
        )
    else:
        tenant = conf.get_table('tenants').find({'tenant_name': tenant_name})
        if not tenant:
            secho("No tenant named {} found.".format(tenant_name), fg='red', bold=True)
            sys.exit(1)

        secho("Tenant {s.BRIGHT}{}{s.NORMAL}:".format(tenant_name, **styles))
        echo(json.dumps(tenant, indent=4))


@cli.command()
@click.option(
    '--shared', '-s',
    help="Use a shared configuration. The DB will be stored in user directory for your platform, "
    "typically ~/ (or %USERPROFILE% on Windows). The configuration can thus be shared between "
    "multiple projects.",
    is_flag=True
)
@click.option(
    '--recreate',
    help="Recreate Configuration DB. (Note, it will overwrite existing developer config).",
    is_flag=True
)
@click.option(
    '--run', '-r',
    help="Run a Flask server.",
    is_flag=True
)
def developer(recreate, shared, run):
    """Set up environment for local development.

    Creates or updates a Drift Configuration DB. The current deployable is automatically
    registered.
    """
    logging.basicConfig(level='INFO')
    secho("----------------", bold=True)
    secho("Drift Developer:", bold=True)
    secho("----------------", bold=True)

    domain_name = 'developer'
    tier_name = 'LOCALTIER'
    tenant_name = 'developer' if shared else 'localdev'
    origin_folder = '.driftconfig-' + domain_name

    # This here makes sure no external resources are referenced, like AWS services.
    os.environ['DRIFT_USE_LOCAL_SERVERS'] = '1'

    if shared:
        origin_folder = os.path.join(os.path.expanduser('~'), origin_folder)
    else:
        # Make sure origin folder is ignored in git.
        if os.path.exists('.gitignore'):
            with open('.gitignore', 'r+') as f:
                if origin_folder not in f.read():
                    secho("Note! Adding {} to .gitignore".format(click.style(origin_folder, bold=True)))
                    f.seek(0, os.SEEK_END)  # Need this on Windows
                    f.write("\n# Drift config folder for local development\n{}\n".format(origin_folder))

    origin_folder = os.path.abspath(origin_folder)
    secho("Origin of this developer config is at: ", nl=False)
    secho(origin_folder, bold=True, fg='blue')
    origin = 'file://{}'.format(origin_folder)

    # See if config already exists
    try:
        ts = get_store_from_url(origin)
    except Exception:
        ts = None

    if recreate or ts is None:
        if ts:
            secho(
                "Warning: Overriding existing configuration because of --recreate flag.",
                fg='yellow'
            )

        testhelpers.DOMAIN_NAME = domain_name
        testhelpers.ORG_NAME = 'localorg'
        testhelpers.TIER_NAME = tier_name
        testhelpers.PROD_NAME = 'localproduct'
        testhelpers.TENANT_NAME = 'dev'
        config_size = {
            'num_org': 1,
            'num_tiers': 1,
            'num_deployables': 0,
            'num_products': 1,
            'num_tenants': 0,
        }

        ts = testhelpers.create_test_domain(
            config_size=config_size,
            resources=None,
            resource_attributes={}
        )
        domain = ts.get_table('domain').get()
        domain['origin'] = origin
        domain['display_name'] = "Configuration for local development"
    else:
        secho("Using existing developer config DB.")

    project_dir = '.'

    # TODO: This is perhaps not ideal, or what?
    config_filename = os.path.join(project_dir, 'config', 'config.json')
    config_filename = os.path.expanduser(config_filename)
    if not os.path.exists(config_filename) or not os.path.exists('setup.py'):
        secho(
            "Error: Please run this command from a deployable root directory.",
            fg='red', bold=True)
        sys.exit(1)

    log.info("Loading configuration from %s", config_filename)
    with open(config_filename) as f:
        app_config = json.load(f)

    package_info = _get_package_info(project_dir=project_dir)

    sys.path.insert(0, '.')  # Make project_dir importable.

    register_this_deployable(
        ts=ts,
        package_info=package_info,
        resources=app_config.get("resources", []),
        resource_attributes=app_config.get("resource_attributes", {}),
    )
    deployable_name = package_info['name']

    # Make sure this deployable is associated with the current product
    product = ts.get_table('products').find()[0]
    if deployable_name not in product['deployables']:
        product['deployables'].append(deployable_name)

    register_this_deployable_on_tier(
        ts, tier_name='LOCALTIER', deployable_name=deployable_name)

    # For convenience, register resource default values as well. This
    # is idempotent so it's fine to call it periodically.
    resources = get_tier_resource_modules(
        ts=ts, tier_name='LOCALTIER', skip_loading=False, ignore_import_errors=True)

    # See if there is any attribute that needs prompting,
    # Any default parameter from a resource module that is marked as <PLEASE FILL IN> and
    # is not already set in the config, is subject to prompting.
    tier = ts.get_table('tiers').get({'tier_name': 'LOCALTIER'})
    config_resources = tier.get('resources', {})
    USE_EMPTY_AS_DEFAULT = True  # Just to slide through without interruptions.

    for resource in resources:
        for k, v in resource['default_attributes'].items():
            if v == "<PLEASE FILL IN>":
                # Let's prompt if and only if the value isn't already set.
                attributes = config_resources.get(resource['module_name'], {})
                if k not in attributes or attributes[k] == "<PLEASE FILL IN>":
                    attrib_name = '{}.{}'.format(resource['module_name'], k)
                    if attrib_name == 'drift.core.resources.redis.host':
                        value = 'localhost'
                    elif attrib_name == 'drift.core.resources.postgres.server':
                        value = 'localhost'
                    elif USE_EMPTY_AS_DEFAULT:
                        value = ''
                    else:
                        echo("Enter value for {s.BRIGHT}{}{s.NORMAL}: ".format(
                            attrib_name, **styles), nl=False)
                        value = input()
                    resource['default_attributes'][k] = value

    register_tier_defaults(ts=ts, tier_name='LOCALTIER', resources=resources)

    secho("\nDefault values for resources configured for this tier:", bold=True)
    secho(pretty(tier.get('resources', {})))

    # Add the developer tenant
    result = define_tenant(
        ts=ts,
        tenant_name=tenant_name,
        product_name=product['product_name'],
        tier_name=tier_name,
    )

    # Define an alias for the developer tenant so it less cumbersome than the actual name.
    result['tenant_master_row']['alias'] = tenant_name
    tenant_name = result['tenant_master_row']['tenant_name']  # The actual tenant name

    # Provision all resources for the tenant for all deployables
    report = provision_tenant_resources(
        ts=ts,
        tenant_name=tenant_name,
    )

    secho("Provisioned tenant", bold=True)
    secho(pretty(report))

    # Save it locally
    domain_folder = config_dir(domain_name)
    local_store = create_backend('file://' + domain_folder)
    local_store.save_table_store(ts)
    secho("New config for '{}' saved to {}.".format(domain_name, domain_folder))
    result = push_to_origin(ts, _first=True)
    if not result['pushed']:
        secho("Push failed. Reason:" + result['reason'], fg='red')

    secho("\nTo enable local development:\n", bold=True)
    sh = textwrap.dedent("""\
        # The following environment variables define full context for local development
        export DRIFT_CONFIG_URL={}
        export DRIFT_TIER={}
        export DRIFT_DEFAULT_TENANT={}
        export FLASK_APP=drift.contrib.flask.devapp:app
        export FLASK_ENV=development

        # To run a flask development server:
        flask run
        """).format(domain_name, tier_name, tenant_name)
    secho(pretty(sh, lexer='bash'))

    # pygments is optionally used for syntax highlightling
    if not got_pygments:
        secho(textwrap.dedent("""\


            Final Note! All the blurb above would look much better with colors!.
            "Plese Run the following command for the sake of rainbows and unicorns:
            "pip install pygments

            """))

    if run:
        # Run a Flask development server
        env = os.environ.copy()
        env['DRIFT_CONFIG_URL'] = domain_name
        env['DRIFT_TIER'] = tier_name
        env['DRIFT_DEFAULT_TENANT'] = tenant_name
        env['FLASK_APP'] = 'drift.contrib.flask.devapp:app'
        # allow the user to override the environment, otherwise default to a development env.
        env['FLASK_ENV'] = os.environ.get('FLASK_ENV') or 'development'

        args = ['flask', 'run', '--host=0.0.0.0']
        port = app_config.get('PORT')
        if port is not None:
            args.append('--port={}'.format(port))

        secho(

            "*" * 60 + "\n"
            "* Running on http://0.0.0.0:{}/ (Press CTRL+C to quit)  *\n".format(port or 5000) +
            "*" * 60 + "\n",
            bold=True, fg='red')

        p = subprocess.Popen(
            args,
            cwd=project_dir,
            env=env
        )
        out, err = p.communicate()
        if p.returncode != 0:
            if six.PY3 and err:
                err = err.decode("utf-8")
            raise RuntimeError(
                "Flask run failed: {} - {}".format(p.returncode, err)
            )


@cli.command()
@click.argument('table-name')
def edit(table_name):
    """Edit a config table.\n
    TABLE_NAME is one of: domain, organizations, tiers, deployable-names, deployables,
    products, tenant-names, tenants.
    """
    ts, source = get_default_drift_config_and_source()
    table = ts.get_table(table_name)
    backend = create_backend(source)
    path = backend.get_filename(table.get_filename())
    with open(path, 'r') as f:
        text = click.edit(f.read())
    if text:
        secho("Writing changes to " + path)
        with open(path, 'w') as f:
            f.write(text)

        _epilogue(ts)


@cli.group()
@pass_repo
def tier(repo):
    """Manage tier related entries in the configuration database."""


@tier.command("info")
@click.option('--tier-name', '-t', type=str, default=None)
def tier_info(tier_name):
    """Show tier info."""
    conf = get_default_drift_config()
    _header(conf)
    if tier_name is None:
        echo("Tiers:")
        tabulate(['tier_name', 'state', 'is_live'], conf.get_table('tiers').find(), indent='  ')
    else:
        tier = conf.get_table('tiers').find({'tier_name': tier_name})
        if not tier:
            secho("No tier named {} found.".format(tier_name), fg='red', bold=True)
            sys.exit(1)
        tier = tier[0]
        echo("Tier {}:".format(tier['tier_name']))
        echo(pretty(tier))


@tier.command("add")
@click.argument('tier-name', type=str)
@click.option('--is-live/--is-dev', help="Flag tier for 'live' or 'development' purposes. Default is 'live'.")
@click.option('--edit', '-e', help="Use editor to modify the entry.", is_flag=True)
def tier_add(tier_name, is_live, edit):
    """Add a new tier.\n
    TIER_NAME is a 3-20 character long upper case string containing only the letters A-Z."""
    with TSLocal() as ts:
        tiers = ts.get_table('tiers')
        entry = {'tier_name': tier_name, 'is_live': is_live}
        if edit:
            edit = click.edit(json.dumps(entry, indent=4))
            if edit:
                entry = json.loads(edit)
        if tiers.find(entry):
            secho("Tier {} already exists!".format(entry['tier_name']), fg='red', bold=True)
            sys.exit(1)
        tiers.add(entry)

        _epilogue(ts)


@tier.command("edit")
@click.argument('tier-name', type=str)
def tier_edit(tier_name):
    """Edit a tier."""
    with TSLocal() as ts:
        tiers = ts.get_table('tiers')
        entry = tiers.get({'tier_name': tier_name})
        if not entry:
            secho("tier {} not found!".format(tier_name))
            sys.exit(1)

        edit = click.edit(json.dumps(entry, indent=4))
        if edit:
            entry = json.loads(edit)
            tiers.update(entry)


@cli.group()
@pass_repo
def organization(repo):
    """Manage organizations in the configuration database."""


@organization.command("info")
@click.option(
    '--name', '-n', 'organization_name', type=str,
    help="Show full info for given organization. Specify name or short name.")
def org_info(organization_name):
    """Show organization info."""
    conf = get_default_drift_config()
    _header(conf)

    if organization_name is None:
        tabulate(
            ['organization_name', 'short_name', 'state', 'display_name'],
            conf.get_table('organizations').find(),
            indent='  ',
        )
    else:
        org = conf.get_table('organizations').find({'organization_name': organization_name})
        if not org:
            org = conf.get_table('organizations').find({'short_name': organization_name})
        if not org:
            secho("No organization named {} found.".format(organization_name), fg='red', bold=True)
            sys.exit(1)
        org = org[0]
        echo("Organization {}:".format(org['organization_name']))
        echo(json.dumps(org, indent=4))


@organization.command("add")
@click.argument('organization-name', type=str)
@click.argument('short-name', type=str)
@click.option('--display-name', '-d', help="Display name.", type=str)
@click.option('--edit', '-e', help="Use editor to modify the entry.", is_flag=True)
def org_add(organization_name, short_name, display_name, edit):
    """Add a new organization.\n
    ORGANIZATION_NAME is a 2-20 character long string containing only lower case letters and digits.\n
    SHORT_NAME is a 2-20 character long string containing only lower case letters and digits."""
    with TSLocal() as ts:
        organizations = ts.get_table('organizations')
        entry = {
            'organization_name': organization_name,
            'short_name': short_name,
        }
        if display_name:
            entry['display_name'] = display_name

        if edit:
            edit = click.edit(json.dumps(entry, indent=4))
            if edit:
                entry = json.loads(edit)
        if organizations.find(entry):
            secho("Organization {} already exists!".format(entry['organization_name']), fg='red', bold=True)
            sys.exit(1)
        organizations.add(entry)

        _epilogue(ts)


@organization.command("edit")
@click.argument('organization-name', type=str)
def org_edit(organization_name):
    """Edit a organization."""
    with TSLocal() as ts:
        organizations = ts.get_table('organizations')
        entry = organizations.get({'organization_name': organization_name})
        if not entry:
            secho("organization {} not found!".format(organization_name))
            sys.exit(1)

        edit = click.edit(json.dumps(entry, indent=4))
        if edit:
            entry = json.loads(edit)
            organizations.update(entry)


@cli.group()
@pass_repo
def product(repo):
    """Manage products in the configuration database."""


@product.command("info")
@click.option('-name', '-n', 'product_name', type=str, help="Show full info for given product.")
def product_info(product_name):
    """Show product info."""
    conf = get_default_drift_config()
    _header(conf)

    if product_name is None:
        tabulate(
            ['organization_name', 'product_name', 'state', 'deployables'],
            conf.get_table('products').find(),
            indent='  ',
        )
    else:
        product = conf.get_table('products').find({'product_name': product_name})
        if not product:
            secho("No product named {} found.".format(product_name), fg='red', bold=True)
            sys.exit(1)
        product = product[0]
        secho("Product {s.BRIGHT}{}{s.NORMAL}:".format(product['product_name'], **styles))
        echo(json.dumps(product, indent=4))


@product.command("add")
@click.argument('product-name', type=str)
@click.option('--edit', '-e', help="Use editor to modify the entry.", is_flag=True)
def product_add(product_name, edit):
    """Add a new product.\n
    PRODUCT_NAME is a 3-35 character long string containing only lower case letters digits and dashes.
    The product name must be prefixed with the organization short name and a dash.
    """
    if '-' not in product_name:
        secho(
            "Error: The product name must be prefixed with the organization "
            "short name and a dash.", fg='red', bold=True)
        sys.exit(1)

    short_name = product_name.split('-', 1)[0]
    conf = get_default_drift_config()
    org = conf.get_table('organizations').find({'short_name': short_name})
    if not org:
        secho("No organization with short name {} found.".format(short_name), fg='red', bold=True)
        sys.exit(1)

    organization_name = org[0]['organization_name']

    with TSLocal() as ts:
        products = ts.get_table('products')
        entry = {
            'organization_name': organization_name,
            'product_name': product_name
        }

        if edit:
            edit = click.edit(json.dumps(entry, indent=4))
            if edit:
                entry = json.loads(edit)
        if products.find(entry):
            secho("Product {} already exists!".format(entry['product_name']), fg='red', bold=True)
            sys.exit(1)
        products.add(entry)

        _epilogue(ts)


@product.command("edit")
@click.argument('product-name', type=str)
def product_edit(product_name):
    """Edit a product."""
    with TSLocal() as ts:
        products = ts.get_table('products')
        entry = products.get({'product_name': product_name})
        if not entry:
            secho("product {} not found!".format(product_name))
            sys.exit(1)

        edit = click.edit(json.dumps(entry, indent=4))
        if edit:
            entry = json.loads(edit)
            products.update(entry)


@cli.command(help="View contents of tables. Specify 'all' to view all tables.")
@click.argument('table-name', default='')
@click.option('--tier-name', '-t', 'tier_name', type=str, help="Filter tier.")
@click.option('--tenant-name', '-n', 'tenant_name', type=str, help="Filter tenant.")
@click.option('--deployable-name', '-d', 'deployable_name', type=str, help="Filter deployable.")
def view(table_name, tier_name, tenant_name, deployable_name):
    c = get_default_drift_config()
    if not table_name:
        secho("Specify one the following table names to view, or 'all' for all tables.")
        for table in c.tables:
            secho("\t{}".format(table))
    else:
        def filter(table):
            f = {}
            if tier_name and 'tier_name' in table._pk_fields:
                f['tier_name'] = tier_name
            if tenant_name and 'tenant_name' in table._pk_fields:
                f['tenant_name'] = tenant_name
            if deployable_name and 'deployable_name' in table._pk_fields:
                f['deployable_name'] = deployable_name
            return f

        if table_name == 'all':
            d = {table.name: table.find(filter(table)) for table in c.tables.values()}
        else:
            table = c.get_table(table_name)
            d = table.find(filter(table))

        echo(pretty(d))


@cli.command(help="Set a custom value. Specify either key=value or a json doc.")
@click.argument('key-value')
@click.option('--location', '-l', 'location', type=str, help="Location of entry.\n"
    "If not set the custom value is stored in 'domain' table.\n"
    "For 'tiers' table specify <tier name>\n"
    "For 'tenants' table specify <tenant name>\n"
    "For 'deployables' table specify <tier name>.<deployable name>\n"
    "For any other table specify <table name>[.<pk value 1>.<pk value 2>...]"
    )
@click.option('--raw', '-r', is_flag=True, help="Insert value straight into row.")
@click.option('--preview', '-p', is_flag=True, help="Do not commit changes to config.")
def set(key_value, location, raw, preview):
    try:
        kv = json.loads(key_value)
        if not isinstance(kv, dict):
            secho("Specify either key=value or a json doc.", fg='red')
            sys.exit(1)
    except Exception as e:
        if "=" in key_value:
            key, val = key_value.split('=', 1)
            kv = {key: val}
        else:
            secho("Error: {}".format(e), fg='red')
            sys.exit(1)

    with TSTransaction(commit_to_origin=not preview) as ts:
        if location:
            # Is it tiers?
            table = ts.get_table('tiers')
            row = table.get({'tier_name': location})

            if not row:
                # Is it tenants?
                table = ts.get_table('tenants')
                rows = table.find({'tenant_name': location})
                if rows:
                    row = rows[0]  # We exploit the fact that the tenant name is unique here.

            if not row and '.' in location:
                # Is it deployables?
                table = ts.get_table('deployables')
                tier_name, deployable_name = location.split('.', 1)
                row = table.get({'tier_name': tier_name, 'deployable_name': deployable_name})

            if not row:
                # Is it name of a table?
                if '.' in location:
                    location, filters = location.split('.', 1)
                    filters = filters.split('.')
                else:
                    filters = []
                if location in ts.tables:
                    table = ts.get_table(location)
                    if len(table._pk_fields) != len(filters) :
                        secho("Table '{}' primary keys: {}".format(table.name, ', '.join(table._pk_fields)))
                        secho("No primary key specified, exiting...")
                        sys.exit(0)
                    row = table.get(dict(zip(table._pk_fields, filters)))
            if not row:
                secho("Location '{}' not found!".format(location), fg='red')
                secho("Run 'dconf view' to get a list of available tables.")
                sys.exit(1)
        else:
            table = ts.get_table('domain')
            row = table.get()

        if not raw:
            master_row = row
            row = row.setdefault('custom_values', {})
        else:
            master_row = row

        # From https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
        def dict_merge(dct, merge_dct):
            """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
            updating only top-level keys, dict_merge recurses down into dicts nested
            to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
            ``dct``.
            :param dct: dict onto which the merge is executed
            :param merge_dct: dct merged into dct
            :return: None
            """
            for k, v in merge_dct.items():
                if (k in dct and isinstance(dct[k], dict)
                        and isinstance(merge_dct[k], collections.Mapping)):
                    dict_merge(dct[k], merge_dct[k])
                else:
                    dct[k] = merge_dct[k]

        dict_merge(row, kv)
        secho("Adding custom value to table '{}':".format(table.name))
        echo(pretty(master_row))
        if not preview:
            secho("Committing changes to origin...")

    if preview:
        secho("This is only a preview! No changes made.")
    else:
        secho("Changes committed to origin!", fg='green')


def tabulate(headers, rows, indent=None, col_padding=None):
    """Pretty print tabular data."""
    indent = indent or ''
    col_padding = col_padding or 3

    # Calculate max width for each column
    col_size = [[len(h) for h in headers]]  # Width of header cols
    col_size += [[len(str(row.get(h, ''))) for h in headers] for row in rows]  # Width of col in each row
    col_size = [max(col) for col in zip(*col_size)]  # Find the largest

    # Sort rows
    def make_key(row):
        return ":".join([str(row.get(k, '')) for k in headers])

    rows = sorted(rows, key=make_key)

    for row in [headers] + rows:
        echo(indent, nl=False)
        for h, width in zip(headers, col_size):
            if row == headers:
                h = h.replace('_', ' ').title()  # Make header name pretty
                secho(h.ljust(width + col_padding), bold=True, nl=False)
            else:
                fg = 'white' ##fg = 'black' if row.get('active', True) else 'white'
                secho(str(row.get(h, '')).ljust(width + col_padding), nl=False, fg=fg)
        echo()


PRETTY_FORMATTER = 'console256'
PRETTY_STYLE = 'tango'


def pretty(ob, lexer=None):
    """
    Return a pretty console text representation of 'ob'.
    If 'ob' is something else than plain text, specify it in 'lexer'.

    If 'ob' is not string, Json lexer is assumed.

    Command line switches can be used to control highlighting and style.
    """
    if lexer is None:
        if isinstance(ob, six.string_types):
            lexer = 'text'
        else:
            lexer = 'json'

    if lexer == 'json':
        ob = json.dumps(ob, indent=4, sort_keys=True)

    if got_pygments:
        lexerob = get_lexer_by_name(lexer)
        formatter = get_formatter_by_name(PRETTY_FORMATTER, style=PRETTY_STYLE)
        # from pygments.filters import *
        # lexerob.add_filter(VisibleWhitespaceFilter())
        ret = highlight(ob, lexerob, formatter)
    else:
        ret = ob

    return ret.rstrip()
