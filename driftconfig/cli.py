# -*- coding: utf-8 -*-
import os
import os.path
import sys

from driftconfig.relib import create_backend, get_store_from_url
from driftconfig.config import get_drift_table_store


def get_options(parser):

    subparsers = parser.add_subparsers(
        title="Config file management",
        description="These sets of commands help you with setting up configuration for Drift products.",
        dest="command",
    )

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

    p = subparsers.add_parser(
        'list',
        help='List locally stored configurations.',
        description="List out all configuration that are stored locally."
    )

    p = subparsers.add_parser(
        'pull',
        help='Pull config.',
        description="Pull latest configuration from source."
    )
    p.add_argument(
        'domain',
        action='store', nargs='?',
    )

    p = subparsers.add_parser(
        'push',
        help='Push config.',
        description="Push local config to source. Use with causion."
    )
    p.add_argument(
        'domain',
        action='store',
    )

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


def init_command(args):
    print "Initializing config from", args.source
    ts = get_store_from_url(args.source)
    domain_name = ts.get_table('domain')['domain_name']
    print "Config domain name: ", domain_name
    local_store = create_backend('file://~/.drift/config/' + domain_name)
    ts.save_to_backend(local_store)
    print "Config stored at: ", local_store


def _get_domains():
    """Return all config domains stored on local disk."""
    config_folder = os.path.join(os.path.expanduser('~'), '.drift', 'config')
    domains = {}
    for dir_name in os.listdir(config_folder):
        path = os.path.join(config_folder, dir_name)
        if os.path.isdir(path):
            try:
                ts = get_store_from_url('file://' + path)
            except Exception as e:
                print "Note: '{}' is not a config folder, or is corrupt. ({}).".format(path, e)
                continue
            domain = ts.get_table('domain')
            domains[domain['domain_name']] = {'path': path, 'table_store': ts}
    return domains


def list_command(args):
    # Enumerate subfolders at ~/.drift/config and see what's there
    for d in _get_domains().values():
        domain = d['table_store'].get_table('domain')
        print "{}: \"{}\" at {}".format(domain['domain_name'], domain['display_name'], domain['origin'])


def pull_command(args):

    for domain_name, domain_info in _get_domains().items():
        if args.domain and args.domain != domain_name:
            continue
        origin = domain_info['table_store'].get_table('domain')['origin']
        print "Pulling '{}' from {}".format(domain_name, origin)
        ts = get_store_from_url(origin)
        local_store = create_backend('file://' + domain_info['path'])
        ts.save_to_backend(local_store)
        print "Config saved at", domain_info['path']


def push_command(args):
    domain_info = _get_domains().get(args.domain)
    if not domain_info:
        print "Can't push '{}'.".format(args.domain)
        sys.exit(1)

    ts = domain_info['table_store']
    origin = ts.get_table('domain')['origin']
    print "Pushing local config to source", origin
    source_store = create_backend(origin)
    ts.save_to_backend(source_store)
    print "Config pushed."


def create_command(args):

    domain_info = _get_domains().get(args.domain)
    if domain_info:
        print "The domain name specified is taken:"
        domain = domain_info['table_store'].get_table('domain')
        print "{}: \"{}\" at {}".format(domain['domain_name'], domain['display_name'], domain['origin'])
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


def run_command(args):
    fn = globals()["{}_command".format(args.command.replace("-", "_"))]
    fn(args)


def main(as_module=False):
    import argparse
    parser = argparse.ArgumentParser(description="")
    get_options(parser)
    args = parser.parse_args()
    fn = globals()["{}_command".format(args.command.replace("-", "_"))]
    fn(args)


if __name__ == '__main__':
    main(as_module=True)
