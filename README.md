# drift-config


# Redis Cache Daemon Setup
Drift config with an S3 based origin can be cached in a Redis DB with a very high concurrency. An AWS lambda will monitor the S3 bucket and update the cache when there is an update.

The labmda is set up using [Zappa](https://github.com/Miserlou/Zappa). The Zappa config file is generated from a template which sets the S3 bucket name (origin), subnet id's and security group id's of the selected drift tier.

Install zappa

Usage:

```bash
driftconfig makezappa <tier name>
```

A file named `zappa_settings.json` is generated. This file is not part of the project and is excluded from the repository.

Once that is done, use the zappa command line to deploy and update:

For first time deployment on a tier:

```bash
zappa deploy <tier name>
```

Subsequent updates:

```bash
zappa update <tier name>
```

