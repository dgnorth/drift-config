# -*- coding: utf-8 -*-
import urlparse
from datetime import datetime

import boto3
import click
from jinja2 import Environment, PackageLoader
from driftconfig.util import get_drift_config, ConfigNotFound, get_domains
from driftconfig.relib import get_store_from_url
from driftconfig.config import push_to_origin, get_redis_cache_backend


class Globals(object):
    pass

@click.command()
#@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enables verbose mode.')
@click.option('--test', '-t', is_flag=True, help='Run a test suite.')
@click.version_option('1.0')
@click.pass_context
@click.argument('config-urls', type=str, nargs=-1)
def cli(ctx, config_urls, verbose, test):
    """Generate settings for Zappa lambdas.
    pass in bobo
    """
    ctx.obj = Globals()
    domains = get_domains()
    if not config_urls:
        config_urls = domains.keys()  # Use all local configs

    # 'config_urls' contains either urls or domain names. Needs some processing.
    table_stores = []
    for config_url in config_urls:
        if config_url in domains:
            # 'config_url' is a domain name. It should point to a locally stored
            # config, which we must check if it has 'origin' defined.
            ts = domains[config_url]['table_store']
            config_url = ts.get_table('domain').get().get('origin')
            if not config_url:
                click.secho("No origin defined for {}".format(ts))
                continue

        parts = urlparse.urlsplit(config_url)
        if parts.scheme != 's3':
            click.secho("Origin must be on S3, but is {}".format(config_url))
        else:
            click.secho("Fetching {}".format(config_url))
            table_stores.append(get_store_from_url(config_url))

    # Template accepts list of tiers which contains the following attributes:
    # S3_ORIGIN_URL: The config origin.
    # TIER_NAME: Tier name.
    # aws_region: The region where this tier is running.
    # s3_bucket_region: The region of the config origin s3 bucket.
    # bucket_name: Name of the bucket that holds the config.
    # subnets: Array of subnet names.
    # security_groups: Array of sg names.
    tiers = {}
    for ts in table_stores:
        domain = ts.get_table('domain').get()
        for tier in ts.get_table('tiers').find():
            tier_name = tier['tier_name']

            if 'organization_name' not in tier:
                click.secho("Note: Tier {} does not define 'organization_name'. Skipping.".format(tier_name))
                continue

            s3_origin_url = domain['origin']

            if tier_name in tiers:
                click.secho("Error: Duplicate tier names found. Tier '{}' is "
                    "defined in both of the following configs:".format(tier_name), fg='red')
                click.secho("Config A: {}".format(s3_origin_url))
                click.secho("Config B: {}".format(tiers[tier_name]['s3_origin_url']))
                click.secho("'{}' from config B will be skipped, but please fix!".format(tier_name))
                continue

            if 'aws' not in tier or 'region' not in tier['aws']:
                click.secho("Note: Tier {} does not define aws.region. Skipping.".format(tier_name))
                continue

            click.secho("Processing {}".format(tier_name), bold=True)

            # Figure out in which aws region this config is located
            aws_region = tier['aws']['region']
            parts = urlparse.urlsplit(s3_origin_url)
            bucket_name = parts.hostname
            s3_bucket_region = boto3.resource("s3").meta.client.get_bucket_location(
                Bucket=bucket_name)["LocationConstraint"]
            # If 'get_bucket_location' returns None, it really means 'us-east-1'.
            s3_bucket_region = s3_bucket_region or 'us-east-1'

            print "Connecting to AWS region {} to gather subnets and security group.".format(aws_region)
            ec2 = boto3.resource('ec2', aws_region)
            filters = [
                {'Name': 'tag:tier', 'Values':[ tier_name]},
                {'Name': 'tag:Name', 'Values': [tier_name+'-private-subnet-1', tier_name+'-private-subnet-2']}
                ]
            subnets=list(ec2.subnets.filter(Filters=filters))
            subnets = [subnet.id for subnet in subnets]

            filters = [
                {'Name': 'tag:tier', 'Values':[ tier_name]},
                {'Name': 'tag:Name', 'Values': [tier_name+'-private-sg']}
                ]

            security_groups=list(ec2.security_groups.filter(Filters=filters))
            security_groups = [sg.id for sg in security_groups]

            # Sum it up
            tier_args = {
                's3_origin_url': s3_origin_url,
                'tier_name': tier_name,
                'organization_name': tier['organization_name'],
                'aws_region': aws_region,
                's3_bucket_region': s3_bucket_region,
                'bucket_name': bucket_name,
                'subnets': subnets,
                'security_groups': security_groups,
                '_ts': ts,
            }
            tiers[tier_name] = tier_args

    if test:
        _run_sanity_check(tiers)
        return

    env = Environment(loader=PackageLoader('driftconfig', package_path='templates'))
    template = env.get_template('zappa_settings.yml.jinja')
    zappa_settings_text = template.render(tiers=tiers.values())

    from driftconfig.cli import pretty
    print pretty(zappa_settings_text, 'yaml')
    with open('zappa_settings.yml', 'w') as f:
        f.write(zappa_settings_text)

    click.secho("zappa_settings.yml generated. Run 'zappa deploy' or zappa update'"
        " for each of the tiers, or use the -all switch to zap them all.")
    print pretty("Example: zappa update -s zappa_settings.yml -all", 'bash')


def _run_sanity_check(tiers):
    for tier_name, tier in tiers.items():
        ts = tier['_ts']
        print "Testing {} from {}".format(tier_name, ts)

        domain = ts.get_table('domain').get()
        domain['_dctest'] = datetime.utcnow().isoformat() + 'Z'
        result = push_to_origin(ts, force=False)
        if not result['pushed']:
            print "Couldn't run test on {} from {}: {}".format(tier_name, ts, result['reason'])
            continue

        b = get_redis_cache_backend(ts, tier_name)
        if not b:
            print "Couldn't get cache backend on {} from {}.".format(tier_name, ts)
        else:
            try:
                ts2 = b.load_table_store()
            except Exception as e:
                if "Timeout" not in str(e):
                    raise
                print "Cache check failed. Redis connection timeout."
                continue

            domain2 = ts2.get_table('domain').get()
            if domain['_dctest'] != domain2.get('_dctest'):
                print "Cache check failed while comparing {} to {}.".format(domain['_dctest'], domain2.get('_dctest'))


if __name__ == '__main__':
    cli()
