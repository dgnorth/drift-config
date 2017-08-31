# -*- coding: utf-8 -*-
'''
Lambda triggers to keep drift config cached in Redis and such
'''
import os.path

from driftconfig.relib import get_store_from_url, create_backend


def on_config_push(event, context):
    """
    Process config push event
    """

    # Get the uploaded file's information
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    path, filename = os.path.split(key)
    s3_url = 's3://{}/{}'.format(bucket, path)

    if filename != 'table-store.pickle':
        print "Drift config cache trigger ignoring file: ", s3_url
        print "Only 'table-store.pickle' files are loaded."
        return

    # Get table store. We can't just push the file itself as we need the 'cache'
    # information from the config itself.
    ts = get_store_from_url('s3://{}/{}'.format(bucket, path))
    domain = ts.get_table('domain').get()
    if 'cache' not in domain:
        print "Drift config cache trigger ignoring file: ", s3_url
        print "This configuration does not specify a Redis cache."

    cache_url = domain['cache'] + '?prefix={}'.format(domain['domain_name'])
    b = create_backend(cache_url)
    b.save_table_store(ts)
    print "Config {} saved to {}".format(s3_url, cache_url)


'''
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
