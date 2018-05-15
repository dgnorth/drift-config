# -*- coding: utf-8 -*-
import sys
import six.moves.urllib.parse import urlsplit
from datetime import datetime
import subprocess

import boto3
import click
from jinja2 import Environment, PackageLoader
from driftconfig.util import get_default_drift_config
from driftconfig.config import push_to_origin, get_redis_cache_backend
from driftconfig.cli import pretty


class Globals(object):
    pass


@click.command()
@click.option('--verbose', '-v', is_flag=True, help='Enables verbose mode.')
@click.option('--test', '-t', is_flag=True, help='Run a test suite and skip deployment.')
@click.version_option('1.0')
@click.pass_context
@click.argument('config-urls', type=str, nargs=-1)
def cli(ctx, config_urls, verbose, test):
    """Generate settings for Zappa lambdas and deploy to AWS.
    """
    ctx.obj = Globals()

    # Template accepts list of tiers which contains the following attributes:
    # S3_ORIGIN_URL: The config origin.
    # TIER_NAME: Tier name.
    # aws_region: The region where this tier is running.
    # s3_bucket_region: The region of the config origin s3 bucket.
    # bucket_name: Name of the bucket that holds the config.
    # subnets: Array of subnet names.
    # security_groups: Array of sg names.
    tiers = {}
    ts = get_default_drift_config()
    domain = ts.get_table('domain').get()
    for tier in ts.get_table('tiers').find():
        tier_name = tier['tier_name']

        if 'organization_name' not in tier:
            click.secho("Note: Tier {} does not define 'organization_name'.".format(tier_name))

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
        parts = urlsplit(s3_origin_url)
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
            'organization_name': tier.get('organization_name', domain['domain_name']),
            'aws_region': aws_region,
            's3_bucket_region': s3_bucket_region,
            'bucket_name': bucket_name,
            'subnets': subnets,
            'security_groups': security_groups,
            '_ts': ts,
        }
        tiers[tier_name] = tier_args

    env = Environment(loader=PackageLoader('driftconfig', package_path='templates'))
    template = env.get_template('zappa_settings.yml.jinja')
    zappa_settings_text = template.render(tiers=tiers.values())

    print pretty(zappa_settings_text, 'yaml')
    filename = '{}.settings.yml'.format(domain['domain_name'])
    with open(filename, 'w') as f:
        f.write(zappa_settings_text)

    click.secho("\n{} generated.\n".format(click.style(filename, bold=True)))

    if test:
        _run_sanity_check(tiers)
        return

    for tier_name in tiers.keys():
        cmd = ['zappa', 'update', '-s', filename, tier_name]
        click.secho("Running command: {}".format(' '.join(cmd)))
        ret = subprocess.call(cmd)
        if ret == 255:
            cmd = ['zappa', 'deploy', '-s', filename, tier_name]
            click.secho("Running command: {}".format(' '.join(cmd)))
            ret = subprocess.call(cmd)


def _run_sanity_check(tiers):
    for tier_name, tier in tiers.items():
        ts = tier['_ts']
        print "Testing {} from {}".format(tier_name, ts)

        domain = ts.get_table('domain').get()
        domain['_dctest'] = datetime.utcnow().isoformat() + 'Z'
        result = push_to_origin(ts, force=False)
        if not result['pushed']:
            click.secho("Couldn't run test on {} from {}: {}".format(
                tier_name, ts, result['reason']), fg='red', bold=True)
            continue

        b = get_redis_cache_backend(ts, tier_name)
        if not b:
            print "Couldn't get cache backend on {} from {}.".format(tier_name, ts)
        else:
            try:
                ts2 = b.load_table_store()
            except Exception as e:
                if "Redis cache doesn't have" in str(e):
                    click.secho("{}. Possible timeout?".format(e), fg='red', bold=True)
                    continue

                if "Timeout" in str(e):
                    click.secho("Cache check failed. Redis connection timeout.", fg='red', bold=True)
                    continue

                raise

            domain2 = ts2.get_table('domain').get()
            if domain['_dctest'] != domain2.get('_dctest'):
                print "Cache check failed while comparing {} to {}.".format(domain['_dctest'], domain2.get('_dctest'))


if __name__ == '__main__':
    cli()
