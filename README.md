# drift-config


# Redis Cache triggers and tasks
Drift config with an S3 based origin can be cached in a Redis DB with a very high concurrency. An AWS lambda will monitor the S3 bucket and update the cache when there is an update.

The lambda is set up using [Zappa](https://github.com/Miserlou/Zappa). The Zappa config file is generated from a template which sets the S3 bucket name (origin), subnet id's and security group id's of the selected drift tier.

#### Preparing local development environment

Zappa requires Python virtualenv. Here's how to set everything up for OSX:

```bash
brew update
brew install pyenv
brew install pyenv-virtualenv

# Set up shell support. Assuming seashell, adjust to taste.
# See https://github.com/pyenv/pyenv#basic-github-checkout
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.zshrc

# And for pyenv-virtualenv
echo 'eval "$(pyenv init -)"' >> ~/.zshrc
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.zshrc

# Apply changes for the current shell
exec "$SHELL"
```

Now we have pyenv and pyenv-virtualenv installed and configured. Next is to pick a Python version and create a virtualenv for it.

```bash
# Let's pick Python 2.7.13 (If this errors, read the error info!)
pyenv install 2.7.13

# Create virtualenv for this project
pyenv shell 2.7.13
pyenv virtualenv drift-config-env

# Make this new virtualenv the default one for our project.
# Make sure to execute the following line in the root of drift-config project!
pyenv local drift-config-env

# Restart you shell or do some exec "$SHELL" magic or something.

# pip install requirements, but skip eggs because they break
pip install --no-binary egg -r requirements.txt

# Install project dependencies into the virtualenv
python setup.py develop

# And finally install zappa into the virtualenv
pip install zappa
```

#### Deploying the lambda code

Run `python generate_settings.py` script found under */scripts*. It will generate a Zappa settings file called *zappa_settings.yml* which includes cache update triggers and tasks for all tiers.

To deploy for the first time, run:

```bash
pyenv local drift-config-env
zappa deploy -s zappa_settings.yml --all
```

If there are changes to any of the tier config that may affect the lambda triggers, or the lambda functions themselves have been changed, the Zappa project needs to be updated on AWS:

```bash
pyenv local drift-config-env
zappa update -s zappa_settings.yml --all
```

