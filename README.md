# drift-config

## Installation
For developer setup [pipenv](https://docs.pipenv.org/) is used to set up virtual environment and install dependencies.

```bash
pip install --user pipenv
pipenv --two
pipenv install -e ".[s3-backend,redis-backend,trigger]"
```
This installs *drift-config* in editable mode with S3 and Redis backend support and lambda trigger support.


## Initialize from origin

If you already have a config db, initialize it for local development. Example:

```bash
driftconfig init s3://some-bucket/config-folder
```


## Install Cache trigger
Drift config with an S3 based origin can be cached in a Redis DB with very high concurrency. An AWS lambda will monitor the S3 bucket and update the cache when there is an update.

The lambda is set up using [Zappa](https://github.com/Miserlou/Zappa). The Zappa config file is generated from a template which sets the S3 bucket name (origin), subnet id's and security group id's of the selected drift tier.

#### Preparing local development environment

To deploy the trigger on AWS, run the following commands:

```bash
python scripts/generate_settings.py
zappa deploy -s zappa_settings.yml --all
```

If there are changes to any of the tier config that may affect the lambda triggers, or the lambda functions themselves have been changed, or the *drift-config* project itself has changed, the trigger may need to be updated on AWS. Run the commands again but use `zappa update` command instead of `zappa deploy`.


