from setuptools import setup

setup(name='suitcase',
      version='0.1.0',
      author='Brookhaven National Laboratory',
      py_modules=['suitcase'],
      description='Tools for exporting data from NSLS-II',
      url='http://github.com/NSLS-II/',
      platforms='Cross platform (Linux, Mac OSX, Windows)',
      requires=['six', 'h5py', 'numpy', 'json', 'dataportal']
      )
