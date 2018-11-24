import re
import ast
from setuptools import setup

_version_re = re.compile(r'__version__\s+=\s+(.*)')


with open('driftconfig/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))


setup(
    name='driftconfig',
    author='Directive Games North',
    version=version,
    url='https://github.com/dgnorth/drift-config',
    packages=['driftconfig'],
    description='Drift Configuration Management.',

    python_requires=">=2.7, !=3.0.*, !=3.1.*",

    # the conditional on i.req avoids the error:
    # distutils.errors.DistutilsError: Could not find suitable distribution for Requirement.parse('None')
    install_requires=[
        'click',
        'jsonschema',
        'jinja2',
        'six',
    ],

    extras_require={
        's3-backend': [
            'boto3',
        ],
        'redis-backend': [
            'redis',
        ],
        'trigger': [
            'boto3',
            'redis',
            'zappa',
        ],
        'testing': [
            'pytest',
            'codecov',
            'pytest-cov',
        ]
    },

    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
    ],
    entry_points='''
        [console_scripts]
        driftconfig=driftconfig.cli:main
        dconf=driftconfig.cli:cli
    '''
)
