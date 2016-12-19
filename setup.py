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
    install_requires=[
        'click>=2.0',
    ],
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
    ],
    entry_points='''
        [console_scripts]
        driftconfig=driftconfig.cli:main
    '''
)
