# -*- coding: utf-8 -*-
'''
AWS lambda functions to keep drift config cached in Redis.
'''
import os
import os.path

from driftconfig.relib import get_store_from_url
from driftconfig.config import update_cache


def on_config_update(event, context):
    """
    Process config update event
    """

    # Get the uploaded file's information
    key = event['Records'][0]['s3']['object']['key']
    path, filename = os.path.split(key)

    if filename != 'table-store.pickle':
        print "Drift config cache trigger ignoring file: ", key
        print "Only 'table-store.pickle' files trigger cache updates."
    else:
        # We don't care if it's this config in particular that got pushed,
        # it's harmless to push the config to cache.
        _push_to_cache(os.environ['S3_ORIGIN_URL'], os.environ['TIER_NAME'])


def do_update_cache(event, context):
    """Update cache specified in env S3_ORIGIN_URL."""
    _push_to_cache(os.environ['S3_ORIGIN_URL'], os.environ['TIER_NAME'])


def _push_to_cache(origin, tier_name):
    """Push config  with origin 'origin' to its designated Redis cache."""
    print "Get config store from url:", origin
    ts = get_store_from_url(origin)
    redis_backend = update_cache(ts, tier_name)
    print "Config {} saved to {}".format(ts, redis_backend)


'''
    import json
    import time
    from driftconfig.relib import get_store_from_url
    def show_domain(cache_url):
        prev_domain = None
        while True:
            ts  = get_store_from_url(cache_url)
            domain = ts.get_table('domain').get()
            if domain != prev_domain:
                print "Table domain in config changed:"
                print json.dumps(domain, indent=4)
                prev_domain = domain
            time.sleep(0.2)



Amazon S3 Put Sample Event
http://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-s3-put

{
  "Records": [
    {
      "eventVersion": "2.0",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "s3": {
        "configurationId": "testConfigRule",
        "object": {
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901",
          "key": "HappyFace.jpg",
          "size": 1024
        },
        "bucket": {
          "arn": bucketarn,
          "name": "sourcebucket",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          }
        },
        "s3SchemaVersion": "1.0"
      },
      "responseElements": {
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
        "x-amz-request-id": "EXAMPLE123456789"
      },
      "awsRegion": "us-east-1",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "eventSource": "aws:s3"
    }
  ]
}
'''
