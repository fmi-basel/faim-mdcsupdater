'''This is a plugin for FAIM-Robocopy that updates file locations of
plate images in an MDCStore database. It supports MS-SQL databases.

The configuration can be set in "mdcstore_updater.ini"

'''
import os
import logging
import configparser
import pyodbc

# NOTE try the relative import in case mldpy is also placed in the
# plugin folder of FAIM-Robocopy.
try:
    from .mldpy import MDCStoreHandle
except ImportError:
    from mldpy import MDCStoreHandle


def get_config(fname):
    '''
    '''
    config = configparser.ConfigParser()
    config.read(fname)
    return {key: val for key, val in config.items(section='MDCStoreUpdater')}


CONFIG_FNAME = os.path.join(os.path.dirname(__file__), 'mdcstore_updater.ini')
CONFIG = get_config(CONFIG_FNAME)


class MDCStoreUpdaterPlugin:
    '''
    '''
    description = 'Update MDCStore'

    tooltip = ('Updates file locations of plate images in the MDCStore '
               'database from <source> to <destination 1>. The update is '
               'performed once at the end of a robocopy task.')

    def __init__(self, shared_resources):
        '''
        '''
        self.shared_resources = shared_resources

    def on_activation(self):
        '''
        '''
        logger = logging.getLogger(__name__)
        logger.info('MDCStore Updater activated. Testing connection...')
        try:
            with MDCStoreHandle(**CONFIG):
                logger.info('MDCStore connection successful.')
        except pyodbc.Error as err:
            logger.error('Connection could not be established: %s', err)

    def on_task_end(self):
        '''
        '''
        logger = logging.getLogger(__name__)

        logger.info('Updating MDCStore...')
        try:
            with MDCStoreHandle(**CONFIG) as db_handle:

                dest = self.shared_resources.dest1_var.get()

                number_of_updated = db_handle.update_file_locations(
                    source=self.shared_resources.source_var.get(), dest=dest)

            logger.info('MDCStore update finished. %s entries were updated.',
                        number_of_updated)
        except Exception as err:
            logger.error('Error during MDCStore update: %s', err)
