'''This is a plugin for FAIM-Robocopy that updates file locations of
plate images in an MDCStore database. It supports MS-SQL databases.

The configuration can be set in "mdcstore_updater.ini"

'''
import os
import logging
import configparser
import pyodbc

from tkinter import Label, Entry, Toplevel
import tkinter.simpledialog

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


class CredentialsDialog(tkinter.simpledialog.Dialog):
    def __init__(self,
                 title,
                 initial_user=None,
                 initial_password=None,
                 parent=None):
        '''
        '''
        # NOTE default parent as in tkinter.simpledialog._QueryDialog
        if parent is None:
            parent = tkinter._default_root
        self.initial_user = initial_user
        self.initial_password = initial_password

        tkinter.simpledialog.Dialog.__init__(self, parent, title)

    def body(self, master):
        '''
        '''
        Label(master, text='Username').grid(row=0)
        Label(master, text='Password').grid(row=1)

        self.username_field = Entry(master)
        self.password_field = Entry(master, show='*')

        if self.initial_user is not None:
            self.username_field.insert(0, self.initial_user)
        if self.initial_password is not None:
            self.password_field.insert(0, self.initial_password)

        self.username_field.grid(row=0, column=1)
        self.password_field.grid(row=1, column=1)

        return self.username_field  # initial focus

    def apply(self):
        '''
        '''
        self.result = {}
        self.result['username'] = self.username_field.get()
        self.result['password'] = self.password_field.get()


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
            self.ask_for_credentials()
            with MDCStoreHandle(**CONFIG):
                self.logger.info('MDCStore connection successful.')
            return True

        except pyodbc.Error as err:
            self.logger.error('Connection could not be established: %s', err)
            return False

    def ask_for_credentials(self):
        '''
        '''
        credentials = CredentialsDialog(
            title='MDCStore Credentials',
            initial_user=CONFIG.get('username', None),
            initial_password=CONFIG.get('password', None)).result
        if credentials is not None:
            CONFIG.update(credentials)

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
