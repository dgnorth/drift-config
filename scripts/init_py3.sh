#
# source this file from the project root to setup a pipenv
# for unit testing with settings for two or three
# eg `. scripts/init_py3.sh`
#

# This is for version three

# remove any old virtualenv
pipenv --rm
rm -f Pipfile

# remove old bytecode
find . -name "*.pyc" -exec rm "{}" ";"

# install again
pipenv --three

#install requirements
pipenv install --dev -e ".[s3-backend,redis-backend,trigger]"