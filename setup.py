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
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
    ],
)
