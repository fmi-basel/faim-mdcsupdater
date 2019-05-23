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
    if not os.path.exists(fname):
        raise RuntimeError(
            'Could not find config for MDCStoreUpdater at {}'.format(fname))
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

    logger = logging.getLogger(__name__)

    def __init__(self, shared_resources):
        '''
        '''
        self.shared_resources = shared_resources

    def on_activation(self):
        '''
        '''
        self.logger.info('MDCStore Updater activated. Testing connection...')
        try:
            with MDCStoreHandle(**CONFIG):
                self.logger.info('MDCStore connection successful.')
        except pyodbc.Error as err:
            self.logger.error('Connection could not be established: %s', err)

    def on_call(self):
        '''
        '''
        source = self.shared_resources.source_var.get()
        dest = self.shared_resources.dest1_var.get()
        self._run(source=source, dest=dest)

    def on_task_end(self):
        '''
        '''
        source = self.shared_resources.source_var.get()
        dest = self.shared_resources.dest1_var.get()
        self._run(source=source, dest=dest)

    def _run(self, source, dest):
        '''
        '''
        for key, path in {'source': source, 'destination': dest}.items():
            if path == '':
                self.logger.error(
                    'Cannot update MDCStore with no %s specified.', key)
                return

        self.logger.info('Updating MDCStore...')
        try:
            with MDCStoreHandle(**CONFIG) as db_handle:

                number_of_updated = db_handle.update_file_locations(
                    source=source, dest=dest)

            self.logger.info(
                'MDCStore update finished. %s entries were updated.',
                number_of_updated)
        except Exception as err:
            self.logger.error('Error during MDCStore update: %s', err)
