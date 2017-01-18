# -*- coding: utf-8 -*-
import os
import os.path
import sys
from datetime import datetime, timedelta
import time
import json
import getpass
import logging

from driftconfig.relib import create_backend, get_store_from_url, load_meta_from_backend, diff_meta, diff_tables
from driftconfig.config import get_drift_table_store, get_domains, push_to_origin, pull_from_origin, parse_8601
from driftconfig.backends import FileBackend
from driftconfig.util import diff_table_stores

log = logging.getLogger(__name__)


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

    # 'create' command
    p = subparsers.add_parser(
        'create',
        help='Create a new config.',
        description="Create a new config. It will only exist locally until it's pushed."
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
        '--organization',
        default=None,
        action='store', help="The name of the domain owner or organization."
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
    # 'addtenant' command
    p = subparsers.add_parser(
        'addtenant',
        help='Add a new tenant',
    )
    p.add_argument(
        'domain',
    )
    p.add_argument(
        '-n', '--name',
        required=True, help="Short name to identify the tenant.",
    )
    p.add_argument(
        '-t', '--tier',
        required=True, help="The name of the tier."
    )
    p.add_argument(
        '-o', '--organization',
        required=True, help="The name of the organization."
    )
    p.add_argument(
        '-p', '--product',
        required=True, help="The name of the product."
    )
    p.add_argument(
        '--preview',
        action='store_true',
        help="Preview the action."
    )
    p.add_argument(
        '-d', '--deployables',
        nargs='*',
        help="One or more deployables to create the tenant on."
    )


def init_command(args):
    print "Initializing config from", args.source
    ts = get_store_from_url(args.source)
    domain_name = ts.get_table('domain')['domain_name']
    print "Config domain name: ", domain_name
    local_store = create_backend('file://~/.drift/config/' + domain_name)
    ts.save_to_backend(local_store)
    print "Config stored at: ", local_store


def _format_domain_info(domain_info):
    domain = domain_info['table_store'].get_table('domain')
    return "{}: \"{}\" at '{}'. Origin: '{}'".format(
        domain['domain_name'], domain['display_name'], domain_info['path'], domain['origin'])


def list_command(args):
    # Enumerate subfolders at ~/.drift/config and see what's there
    for d in get_domains().values():
        print _format_domain_info(d)


def pull_command(args):
    if args.loop:
        pull_config_loop(args)
    else:
        _pull_command(args)


def pull_config_loop(args):
    print "Starting the pull config loop"
    while now() < end_time:
        st = time.time()
        _pull_command(args)
        diff = time.time() - st
        this_sleep_time = max(sleep_time - diff, 0)
        print "Waiting for %.1f sec" % this_sleep_time
        time.sleep(this_sleep_time)
    print "Completed in %.1f sec" % (now() - start_time).total_seconds()


def _pull_command(args):
    for domain_name, domain_info in get_domains().items():
        if args.domain and args.domain != domain_name:
            continue

        result = pull_from_origin(domain_info['table_store'], ignore_if_modified=args.ignore_if_modified, force=args.force)

        if not result['pulled']:
            print "Pull failed for", domain_name, ". Reason:", result['reason']
            if result['reason'] == 'local_is_modified':
                print "Use --ignore-if-modified to overwrite local changes."
            else:
                print "Use --force to force a pull."
        else:
            local_backend = create_backend('file://' + domain_info['path'])
            result['table_store'].save_to_backend(local_backend)

            print "Config pulled. Reason: ", result['reason']


def migrate_command(args):
    print "Migrating '{}'".format(args.domain)
    path = os.path.expanduser('~') + '/.drift/config/' + args.domain
    if not os.path.exists(path):
        print "Path not found:", path
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
    ts.load_from_backend(local_store, skip_definition=True)
    ts.save_to_backend(local_store)


def now():
    return datetime.utcnow()


sleep_time = 10
run_time = 50

start_time = now()
end_time = start_time + timedelta(seconds=run_time)


def push_command(args):
    domain_info = get_domains().get(args.domain)
    if not domain_info:
        print "Can't push '{}'.".format(args.domain)
        sys.exit(1)

    ts = domain_info['table_store']
    origin = ts.get_table('domain')['origin']
    print "Pushing local config to source", origin
    result = push_to_origin(ts, args.force)
    if not result['pushed']:
        print "Push failed. Reason:", result['reason']
        print "Origin has changed. Use --force to force push."
        if 'time_diff' in result:
            print "Time diff", result['time_diff']
    else:
        print "Config pushed. Reason: ", result['reason']
        local_store = create_backend('file://' + domain_info['path'])
        ts.save_to_backend(local_store)


def create_command(args):

    domain_info = get_domains().get(args.domain)
    if domain_info:
        print "The domain name specified is taken:"
        print _format_domain_info(domain_info)
        sys.exit(1)

    # Get empty table store for Drift.
    ts = get_drift_table_store()
    ts.get_table('domain').add(
        {'domain_name': args.domain, 'origin': args.source, 'display_name': args.organization or ''})

    # Save it locally
    domain_folder = os.path.join(os.path.expanduser('~'), '.drift', 'config', args.domain)
    local_store = create_backend('file://' + domain_folder)
    ts.save_to_backend(local_store)
    print "New config for '{}' saved to {}.".format(args.domain, domain_folder)
    print "You can modify the files now before pushing it to source."


def diff_command(args):
    # Get local table store and its meta state
    domain_info = get_domains().get(args.domain)
    local_ts = domain_info['table_store']
    local_m1, local_m2 = local_ts.refresh_metadata()

    # Get origin table store meta info
    origin = local_ts.get_table('domain')['origin']
    origin_backend = create_backend(origin)
    origin_ts = load_meta_from_backend(origin_backend)
    origin_meta = origin_ts.meta.get()

    local_diff = ("Local store and scratch", local_m1, local_m2, False)
    origin_diff = ("Local and origin", origin_meta, local_m2, args.details)

    for title, m1, m2, details in local_diff, origin_diff:
        diff = diff_meta(m1, m2)
        if diff['identical']:
            print title, "is clean."
        else:
            print title, "are different:"
            if diff['modified_diff']:
                print "\tTime since pull: ", str(diff['modified_diff']).split('.')[0]

            print "\tNew tables:", diff['new_tables']
            print "\tDeleted tables:", diff['deleted_tables']
            print "\tModified tables:", diff['modified_tables']

            if details:
                # Diff origin
                origin_ts = get_store_from_url(origin)
                for table_name in diff['modified_tables']:
                    t1 = local_ts.get_table(table_name)
                    t2 = origin_ts.get_table(table_name)
                    tablediff = diff_tables(t1, t2)
                    print "\nTable diff for", table_name, "\n(first=local, second=origin):"
                    print json.dumps(tablediff, indent=4, sort_keys=True)


def addtenant_command(args):

    print "Adding a new tenant."
    print "  Domain:      ", args.domain
    print "  Tenant:      ", args.name
    print "  Tier:        ", args.tier
    print "  Organization:", args.organization
    print "  Product:     ", args.product
    print "  Deployables: ", args.deployables

    domain_info = get_domains().get(args.domain)
    if not domain_info:
        print "The domain '{}'' is not found locally. Run 'init' to fetch it.".format(args.domain)
        sys.exit(1)

    print _format_domain_info(domain_info)

    ts = domain_info['table_store']
    row = ts.get_table('tenant-names').add({
        'tenant_name': args.name,
        'organization_name': args.organization,
        'product_name': args.product,
        'reserved_by': getpass.getuser(),
        'reserved_at': datetime.utcnow().isoformat(),
    })

    print "\nNew tenant record:\n", json.dumps(row, indent=4)

    if args.deployables:
        print "Associating with deployables:"
        tenants = ts.get_table('tenants')
        for deployable_name in args.deployables:
            row = tenants.add({
                'tier_name': args.tier,
                'tenant_name': args.name,
                'deployable_name': deployable_name
            })
            print json.dumps(row, indent=4)

    if args.preview:
        print "Previewing only. Exiting now."
        sys.exit(0)

    # Save it locally
    local_store = create_backend('file://~/.drift/config/' + args.domain)
    ts.save_to_backend(local_store)
    print "Changes to config saved at {}.".format(local_store)
    print "Remember to push changes to persist them."


def run_command(args):
    fn = globals()["{}_command".format(args.command.replace("-", "_"))]
    fn(args)


def main(as_module=False):
    import argparse
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--loglevel')
    get_options(parser)
    args = parser.parse_args()

    if args.loglevel:
        logging.basicConfig(level=args.loglevel)

    fn = globals()["{}_command".format(args.command.replace("-", "_"))]
    fn(args)


if __name__ == '__main__':
    main(as_module=True)
