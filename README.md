# Drift Config

Drift Multi-Tenant Configuration Database - Drift-MTC.

This library enables multi-tenant software deployment and operation for local and cloud based development.

## Installation
For developer setup [pipenv](https://docs.pipenv.org/) is used to set up virtual environment and install dependencies.

```bash
pip install --user pipenv
pipenv install --dev -e ".[s3-backend,redis-backend,trigger]"
```
This installs *drift-config* in editable mode with S3 and Redis backend support and lambda trigger support.

## Overview

In a nutshell this library is a database containing configuration info for multi-tenant services and applications:

 - Database can be stored on a file system, S3, Redis or in-memory and is extendable to virtually any backend storage solution.
 - The main categories of Drift-MTC:
   - Domain information: Operation environments/tiers (localhost, AWS, dev, prod, etc..), global configuration.
   - Application and services: Default resources (like DBMS), global and tenant specific configuration. 
   - Organizations, product definitions and tenants.
 - Database origin normally on S3 but a copy can be pulled locally (similar to git).
 - Database engine runs on ReLib - Lightweight Relational Database Library for Json docs.
   - Relational schema describes all data structures and relational integrity is maintained.
   - Contents of the database is human readable. Modifications and batch updates can be made directly on the files.
   - Application specific schemas can be defined and the DB extended.
 - High performance binary fromat of the DB cached on Redis for optimal access time.
   - Redis cache can be compiled and updated automatically from origin using an S3 lambda trigger.


# Command Line Interface

Due to reasons there are two CLI apps, `driftconfig` and `dconf`. The former is the original CLI based on the [argparse](https://docs.python.org/3/library/argparse.html) module but and effort was made to migrate it over to [click](https://click.palletsprojects.com/). This work is halfway done so we are stuck with two CLI's at the moment.

**Note!** Have all the CLI output colorized: `pip install pygments`


## The "Get me up and running now!" tutorial:

Once installed, initialize a DB from S3 using this command (using Directive Games as an example):

```bash
driftconfig init s3://relib-test/directive-games
```

To see what's currently in the DB run the following commands one at a time. This also helps with seeing how things are tied together.

```bash
dconf tier info
dconf organization info
dconf product info

dconf list configs
dconf list tenants
dconf list deployables

# Dump out the contents of the 'tiers' table:
dconf view tiers
```

...more coming soon.

### Using the library
To use this library in this form, make sure you have your virtualenv activated: `pipenv shell`

### Running unittests
```bash
pipenv run pytest
```

### Python 2/3 compatibility
Set up the virtualenv by adding `--three` or `--two` to the `pipenv install` command line.  The syntax to run the unittests for either version of python is the same.

When switching between version, you need to remove the `Pipfile` that gets created from the `setup.py` file since
it contains the python version and isn't automatically owerwritten when a new environment is created.
You may also need to remove `.pyc` files, e.g. with a command such as:
```bash
find . -name "*.pyc" --exec rm "{}" ";"
```

The steps to switch between python versions are encapsulated in the files `scripts/init_py2.sh` and
`scripts/init_py3.sh`.  To switch between versions, _cd_ to the root folder and source the scripts.
E.g.:
```bash
cd drift-config
. scripts/init_py3.sh
```

## Initialize from url

If you already have a config db, initialize it for local development. Example:

```bash
driftconfig init s3://some-bucket/config-folder
```

## WARNING - OLD STUFF BELOW: Usage

Run `driftconfig --help` for help on usage.

###### Errata: Run `dconf --help` as well.

## Install Cache trigger
Drift config with an S3 based origin can be cached in a Redis DB with very high concurrency. An AWS lambda will monitor the S3 bucket and update the cache when there is an update.

The lambda is set up using [Zappa](https://github.com/Miserlou/Zappa). The Zappa config file is generated from a template which sets the S3 bucket name (origin), subnet id's and security group id's of the selected drift tier.


To update (or deploy) the trigger on AWS, run the following commands:

```bash
python scripts/update-trigger.py
```




