import versioneer
from setuptools import setup

setup(name='suitcase',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      author='Brookhaven National Laboratory',
      py_modules=['suitcase'],
      description='Tools for exporting data from NSLS-II',
      url='http://github.com/NSLS-II/',
      platforms='Cross platform (Linux, Mac OSX, Windows)',
      requires=['six', 'h5py', 'numpy', 'json', 'dataportal']
      )
