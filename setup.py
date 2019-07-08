from setuptools import setup, find_packages
import sys
import os
import re

if sys.version_info <= (2, 7):
    sys.exit('Python 2.7 is not supported')


def parse_version(fname):
    '''parse the version in case the direct import does not work.

    '''
    with open(fname, 'r') as fin:
        version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                                  fin.read(), re.M)
        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


try:
    from mldpy import __version__
except ModuleNotFoundError:
    __version__ = parse_version(os.path.join('mldpy', '__init__.py'))

contrib = ['Markus Rempfler']

setup(name='faim-mdcsupdater',
      version=__version__,
      description='Updater for file locations in MDCStore databases.',
      packages=find_packages(),
      scripts=['run_updater.py'],
      install_requires=['pyodbc'])
