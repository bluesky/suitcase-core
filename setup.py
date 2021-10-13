from os import path
from setuptools import setup
import sys


class IncompatiblePackageError(Exception):
    pass

# Make sure that the unrelated package by the name 'suitcase' is *not*
# installed because if it is installed it will break this suitcase's namespace
# package scheme.
try:
    import suitcase
except ImportError:
    pass
else:
    if hasattr(suitcase, '__file__'):
        raise IncompatiblePackageError(
            "The package 'suitcase' must be uninstalled before "
            "'suitcase-core' can be installed. The package distributed under "
            "the name 'suitcase' is an unrelated project, and it creates "
            "conflicts with suitcase-core's namespace packages.")

# NOTE: This file must remain Python 2 compatible for the foreseeable future,
# to ensure that we error out properly for people with outdated setuptools
# and/or pip.
min_version = (3, 6)
if sys.version_info < min_version:
    error = """
suitcase does not support Python {0}.{1}.
Python {2}.{3} and above is required. Check your Python version like so:

python3 --version

This may be due to an out-of-date pip. Make sure you have pip >= 9.0.1.
Upgrade pip like so:

pip install --upgrade pip
""".format(*sys.version_info[:2], *min_version)
    sys.exit(error)

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as readme_file:
    readme = readme_file.read()

with open(path.join(here, 'requirements.txt')) as requirements_file:
    # Parse requirements.txt, ignoring any commented-out lines.
    requirements = [line for line in requirements_file.read().splitlines()
                    if not line.startswith('#')]


setup(
    name='suitcase-core',
    version='0.9.1',
    description="Exporters / serializers for bluesky documents.",
    long_description=readme,
    author="Brookhaven National Lab",
    author_email='dallan@bnl.gov',
    url='https://github.com/NSLS-II/suitcase',
    packages=[],  # This is a namespace package with dependencies and docs.
    entry_points={
        'console_scripts': [
            # 'some.module:some_function',
            ],
        },
    include_package_data=True,
    package_data={
        'suitcase': [
            # When adding files here, remember to update MANIFEST.in as well,
            # or else they will not be included in the distribution on PyPI!
            # 'path/to/data_file',
            ]
        },
    install_requires=requirements,
    license="BSD (3-clause)",
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
    ],
)
