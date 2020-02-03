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


### Using the library

The config can be accessed directly from S3 or Redis cache but for this tutorial we will pull the config and store it on a local hard drive. (Note that the following S3 url only worksfor members of Directive Games organization):

```bash
pipenv shell
driftconfig init s3://relib-test/directive-games

# To get future updates of the config just run following command:
driftconfig pull dgnorth
```

Now the config DB is stored locally. Run the following command and note the **Local** file path:

```bash
dconf list configs
```

To use the Python library start an interpreter:

```bash
pipenv run python
```

First let's list out some fun stuff. Run only one command at a time otherwise you will just get a big wall of text with no context.

```python
# Specify which Drift config to use. Only needed if there are more than one stored locally.
# Note that this environment variable is always set on EC2 instances during launch and typically
# points to a Redis cache.
import os
os.environ["DRIFT_CONFIG_URL"] = "dgnorth"

# Now grab the config DB in its entirety
from driftconfig.util import get_default_drift_config
c = get_default_drift_config()

# Show list of table names
c.tables.keys()

# A table has a get() and find() function to fetch a row specifying the primary key and to select
# multiple rows based on a search criteria respectively.
tiers = c.get_table("tiers")
tiers.find()  # No search criteria returns all rows.

# Get the row for DEVNORTH
tiers.get({"tier_name": "DEVNORTH"})

# Now let's get some tenant info. This here will return config for all deployables associated with
# the tenant 'dg-oasis' on DEVNORTH:
tenants = c.get_table("tenants")
tenants.find({"tier_name": "DEVNORTH", "tenant_name": "dg-oasis"})

# It's possible to cheat a little bit and do some introspection. Here we can find the primary key
# definition for tenants:
tenants._pk_fields

# The primary in 'tenants' table is composed of three fields: tier_name, deployable_name and tenant_name.
# We can thus get a specific record using all three fields:
tenants.get({"tier_name": "DEVNORTH", "tenant_name": "dg-oasis", "deployable_name": "drift-base"})

# Every bit of info in Drift Config DB should be easily queryable using simple commands as shows above. There
# is however also another helper function that goes one step further.
from driftconfig.util import get_drift_config
c = get_drift_config(tier_name="DEVNORTH")  # We must specify the tier here, usually found in DRIFT_TIER env var.

# str-ifying 'c' yields a lot of info:
c

# Normally this function is used in conjunction with a specific tenant and a deployable/app. A request comes
# through some endpoint and we need the configuration context for it:
c = get_drift_config(tier_name="DEVNORTH", deployable_name="drift-base", tenant_name="dg-oasis")

# Now we have everything at our fingertips using a dot-notation syntax:
c.tier
c.deployable
c.tenant
c.product
c.organization
c.domain.get()
c.source
```

That's all!


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




