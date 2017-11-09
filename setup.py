import re
import ast
from setuptools import setup
from pip.req import parse_requirements
import pip.download

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

    # the conditional on i.req avoids the error:
    # distutils.errors.DistutilsError: Could not find suitable distribution for Requirement.parse('None')
    install_requires=[
        str(i.req)
        for i in parse_requirements('requirements.txt', session=pip.download.PipSession())
        if i.req
    ],
    tests_require=[
        'werkzeug',
    ]
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
