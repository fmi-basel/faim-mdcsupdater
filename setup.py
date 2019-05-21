from setuptools import setup, find_packages
from mldpy import __version__
import sys

if sys.version_info <= (2, 7):
    sys.exit('Python 2.7 is not supported')

contrib = ['Markus Rempfler']

setup(name='faim-mdcsupdater',
      version=__version__,
      description='Updater for file locations in MDCStore databases.',
      packages=find_packages(),
      scripts=['run_updater.py'],
      install_requires=['pyodbc'])
