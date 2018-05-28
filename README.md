# drift-config

## Installation
For developer setup [pipenv](https://docs.pipenv.org/) is used to set up virtual environment and install dependencies.

```bash
pip install --user pipenv
pipenv install --dev -e ".[s3-backend,redis-backend,trigger]"
```
This installs *drift-config* in editable mode with S3 and Redis backend support and lambda trigger support.

### Using the library
To use this library in this form, make sure you have your virtualenv activated: `pipenv shell`

### Running unittests
```bash
pipenv run python -m unittest discover
```

### Python 2/3 compatibility
Set up the virtualenv by adding `--three` or `--two` to the `pipenv install` command line.  The syntax to run the unittests for either version of python is the same.

When switching between version, you need to remove the `Pipfile` that gets created from the `setup.py` file since
it contains the python version and isn't automatically owerwritten when a new environment is created.
You may also need to remove `.pyc` files, e.g. with a command such as: 
```bash 
find . -name "*.pyc" --exec rm "{}" ";" 
``` 

## Initialize from url

If you already have a config db, initialize it for local development. Example:

```bash
driftconfig init s3://some-bucket/config-folder
```

## Usage

Run `driftconfig --help` for help on usage.

###### Errata: Run `dconf --help` as well.

## Install Cache trigger
Drift config with an S3 based origin can be cached in a Redis DB with very high concurrency. An AWS lambda will monitor the S3 bucket and update the cache when there is an update.

The lambda is set up using [Zappa](https://github.com/Miserlou/Zappa). The Zappa config file is generated from a template which sets the S3 bucket name (origin), subnet id's and security group id's of the selected drift tier.


To update (or deploy) the trigger on AWS, run the following commands:

```bash
python scripts/update-trigger.py
```



