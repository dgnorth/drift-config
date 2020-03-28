import re
import ast
from setuptools import setup

_version_re = re.compile(r'__version__\s+=\s+(.*)')


with open('driftconfig/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))


setup(
    name='python-driftconfig',
    author='Directive Games',
    version=version,
    url='https://github.com/dgnorth/drift-config',
    author_email='info@directivegames.com',
    packages=['driftconfig'],
    description='Drift Configuration Management.',

    python_requires=">=3.6",

    # the conditional on i.req avoids the error:
    # distutils.errors.DistutilsError: Could not find suitable distribution for Requirement.parse('None')
    install_requires=[
        'click',
        'jsonschema',
        'jinja2',
        'six',
        'cachetools',
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
        'test': [
            'pytest',
            'pytest-cov',
        ]
    },
    entry_points='''
        [console_scripts]
        driftconfig=driftconfig.cli:main
        dconf=driftconfig.cli:cli
    ''',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Flask',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
