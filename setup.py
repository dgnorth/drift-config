import os
from setuptools import setup

version_file = os.path.abspath(os.path.join("driftconfig", "VERSION"))

with open(version_file) as f:
    version = f.readlines()[0].strip()


setup(
    name='python-driftconfig',
    version=version,
    license='MIT',
    author='Directive Games',
    author_email='info@directivegames.com',
    description='Drift Configuration Management.',
    packages=['driftconfig'],
    url='https://github.com/dgnorth/drift-config',
    include_package_data=True,

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
            'pytest>=5.0',
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
