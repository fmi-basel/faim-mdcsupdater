import os
import re
import socket
import logging
import pathlib

from itertools import tee
from enum import Enum

import pyodbc


def is_subdir(parent, potential_child):
    '''
    '''
    parent = os.path.normpath(os.path.realpath(parent))
    potential_child = os.path.normpath(os.path.realpath(potential_child))
    return os.path.commonprefix([parent, potential_child]) == parent


def get_host():
    '''
    '''
    return socket.gethostname()


def is_consecutive(values):
    '''checks if values are consecutive with a delta of 1.

    '''
    first_iter, second_iter = tee(values)
    next(second_iter, None)  # advance the second iterator by one.
    for first_val, second_val in zip(first_iter, second_iter):
        if first_val + 1 != second_val:
            return False
    return True


def _split_by_mount(input_path):
    '''
    '''
    orig_path = path = os.path.realpath(input_path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    return path, orig_path[len(path):]


def file_location_from_path(path):
    '''
    '''
    mount, directory = _split_by_mount(path)

    location = {'DIRECTORY': directory}

    if path.startswith('\\\\'):
        location['LOCATION_TYPE'] = 2
        location['SERVER_NAME'] = mount

    elif re.match('[A-Z]:', path):
        location['LOCATION_TYPE'] = 1
        location['SERVER_NAME'] = get_host()
        location['SERVER_ROOT'] = mount

    else:
        raise NotImplementedError(
            'Unknown file location type: {}'.format(path))
    return location


def _replace_pw(stuff):
    '''replace password in string for logging.

    '''
    return re.sub('PWD=.*;', 'PWD=***;', stuff)


class MDCStoreHandle:
    '''Handles interaction with MCDStore database.

    NOTE Currently, this only implements the update for MDCStore running
    on a MS-SQL database.

    '''
    class LOCATION_COLUMNS(Enum):
        '''Columns refering to FILE_LOCATIONS.

        '''
        OBJ = 'LOCATION_ID'
        THUMB = 'THUMB_LOCATION_ID'

    # Mapping of LocationColumns to respective column name for of the file.
    FNAME_COLUMNS = {
        LOCATION_COLUMNS.OBJ: 'OBJ_SERVER_NAME',
        LOCATION_COLUMNS.THUMB: 'THUMB_SERVER_NAME'
    }

    # logging
    logger = logging.getLogger(__name__)

    def __init__(self,
                 username,
                 password,
                 host='localhost',
                 database='MDCStore',
                 driver=None):
        '''
        '''
        if driver is None:
            driver = 'ODBC Driver 17 for SQL Server'
        if not driver.startswith('{'):
            driver = '{' + driver
        if not driver.endswith('}'):
            driver += '}'
        self._connect_cmd = ('DRIVER={}; '
                             'SERVER={}; '
                             'DATABASE={}; '
                             'UID={}; '
                             'PWD={};'.format(driver, host, database, username,
                                              password))

    def __enter__(self):
        '''
        '''
        self.open()
        return self

    def __exit__(self, *args):
        '''
        '''
        self.close()
        self.db_conn = None

    def open(self):
        '''
        '''
        self.logger.debug('Opening connection to database with %s',
                          _replace_pw(self._connect_cmd))
        self.db_conn = pyodbc.connect(self._connect_cmd, timeout=10)

    def close(self):
        '''
        '''
        self.logger.debug('Closing connection to database.')
        self.db_conn.close()

    def _collect_locations(self, source):
        '''
        '''
        query = """
        select
            FL.LOCATION_ID,
            case
                when FL.LOCATION_TYPE=1 then CONCAT(FL.SERVER_ROOT, FL.DIRECTORY)
                when FL.LOCATION_TYPE=2 then CONCAT(FL.SERVER_NAME, FL.DIRECTORY)
                else NULL
            end
        from FILE_LOCATION as FL
        """
        with self.db_conn.cursor() as cursor:
            for location_id, path in cursor.execute(query):
                if path is None:
                    continue
                if is_subdir(source, path):
                    yield location_id, path

    def collect_images_at_location(self, location_id, location_type):
        '''yields all images and thumbs with the given location_id.

        '''
        assert location_type in MDCStoreHandle.LOCATION_COLUMNS
        query = """
        select
            OBJ_ID, {}
        from PLATE_IMAGE_DATA
        where
            {}=?
        """.format(MDCStoreHandle.FNAME_COLUMNS[location_type],
                   location_type.value)

        self.logger.debug('Query for images at location %s: %s', location_id,
                          query)
        with self.db_conn.cursor() as cursor:
            for plate_image_id, plate_image_name in cursor.execute(
                    query, location_id):
                yield plate_image_id, plate_image_name

    def update_file_locations(self, source, dest):
        '''check for files that were moved/copied from source/
        to dest/ and update their file location in the database.

        NOTE This considers both OBJ and THUMBS from PLATE_IMAGE_DATA.
        NOTE All checks for file existence are *shallow*.

        '''

        # quickfix to make the path representation uniform.
        source = str(pathlib.Path(source))
        dest = str(pathlib.Path(dest))

        def _source_name(source_dir, obj_name):
            '''compose source path to image.

            '''
            return os.path.join(source_dir, obj_name)

        def _dest_dir(source_dir):
            '''compose destination directory by replacing the
            source/ part of source_dir with dest/.

            '''
            common = os.path.commonprefix([source, source_dir])
            if not common:
                raise ValueError(
                    'Could not determine common path between %s and %s',
                    source, source_dir)
            if not common == source:
                raise ValueError(
                    'source is not identical to common part of path: %s vs %s',
                    common, source)
            remainder = source_dir[len(common):].strip(os.sep)
            return os.path.join(dest, remainder)

        def _file_exists_at_dest(source_dir, obj_name):
            '''compose potential destination path to image and check if it exists.

            '''
            try:
                return os.path.exists(
                    os.path.join(_dest_dir(source_dir), obj_name))
            except Exception as err:
                self.logger.debug(
                    'Could not check if file exists at destination: %s', err)
                return False

        # cache locations in order to be able to free up the db connection.
        number_of_updated = 0
        file_location_candidates = list(self._collect_locations(source))
        self.logger.info(
            'Found %d candidate file locations at the given source. ',
            len(file_location_candidates))
        for location_id, source_dir in file_location_candidates:

            # Check for all columns refering to a location_id.
            for location_type in MDCStoreHandle.LOCATION_COLUMNS:

                self.logger.debug(
                    'Collecting candidates at location %d : %s of type %s.',
                    location_id, source_dir, location_type)

                queue = [
                    obj_id
                    for obj_id, obj_name in self.collect_images_at_location(
                        location_id, location_type)
                    if _file_exists_at_dest(source_dir, obj_name)
                ]
                self.logger.debug('Collected queue of %d candidates',
                                  len(queue))

                self.logger.info('{:>4}:   {}\n\t{} {} in\n\t{}'.format(
                    location_id, source_dir, len(queue), 'files'
                    if location_type == MDCStoreHandle.LOCATION_COLUMNS.OBJ
                    else 'thumbs', _dest_dir(source_dir)))

                if len(queue) >= 1:
                    new_location_id = self._create_new_location(
                        _dest_dir(source_dir))
                    number_of_updated += self._update_multiple_files(
                        queue, new_location_id, location_type)
        return number_of_updated

    def _create_new_location(self, dest_dir):
        '''
        '''
        location = file_location_from_path(dest_dir)

        location_id = self._get_location_id(location)
        if location_id is not None:
            return location_id

        keys, vals = zip(*list(location.items()))

        query = """
        insert into
            FILE_LOCATION ({})
        output INSERTED.LOCATION_ID
        values
            ({})
        """.format(','.join(keys), ','.join('?' for _ in vals))

        self.logger.debug('Query for inserting new location: %s', query)
        with self.db_conn.cursor() as cursor:
            cursor.execute(query, vals)
            location_id = cursor.fetchval()
            return location_id

    def _get_location_id(self, location):
        '''
        '''
        keys, vals = zip(*list(location.items()))

        query = """
        select LOCATION_ID
        from FILE_LOCATION
        where ({})
        """.format(' and '.join('{}=?'.format(key) for key in keys))

        self.logger.debug('Query for location_id: %s', query)
        with self.db_conn.cursor() as cursor:
            cursor.execute(query, vals)
            return cursor.fetchval()

    def _update_multiple_files(self, obj_ids, new_location_id, field_type):
        '''
        '''
        if field_type not in MDCStoreHandle.LOCATION_COLUMNS:
            raise RuntimeError(
                'Unknown field type for update: {}'.format(field_type))

        # sort obj_ids to check if they are consecutive
        obj_ids.sort()

        if is_consecutive(obj_ids):
            condition = 'where OBJ_ID >= {} and OBJ_ID <= {}'.format(
                obj_ids[0], obj_ids[-1])  # min and max.
        else:
            condition = 'where OBJ_ID in ({})'.format(','.join(
                '%d' % obj_id for obj_id in obj_ids))

        query = """
        update
            PLATE_IMAGE_DATA
        set
            {}=?
        """.format(field_type.value) + condition

        self.logger.debug('Query for updating to new location: %s', query)
        with self.db_conn.cursor() as cursor:
            count = cursor.execute(query, new_location_id).rowcount
            self.logger.debug('Updated {} items to {}={}'.format(
                count, field_type.value, new_location_id))
            cursor.commit()
            return count
