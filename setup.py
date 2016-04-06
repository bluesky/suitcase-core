from setuptools import setup, find_packages
import versioneer

setup(
    name='suitcase',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author='Brookhaven National Laboratory',
    packages=find_packages(),
    description='Tools for exporting data from NSLS-II',
    url='http://github.com/NSLS-II/suitcase',
    platforms='Cross platform (Linux, Mac OSX, Windows)',
)
