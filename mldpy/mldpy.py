import os
import re
import socket
import logging

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


class MDCStoreHandle:
    '''Handles interaction with MCDStore database.

    NOTE For now, this only implements MS-SQL versions.

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
                 database='MDCStore'):
        '''
        '''
        self._connect_cmd = (
            'DRIVER={ODBC Driver 17 for SQL Server}; ' +
            'SERVER={} ; DATABASE={} ; UID={} ; PWD={}'.format(
                host, database, username, password))

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
        self.logger.debug('Opening connection to database.')
        self.db_conn = pyodbc.connect(self._connect_cmd, timeout=10)

    def close(self):
        '''
        '''
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

        self.logger.debug('Query for images at location: %s', query)
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

        def _source_name(source_dir, obj_name):
            '''compose source path to image.

            '''
            return os.path.join(source_dir, obj_name)

        def _dest_dir(source_dir):
            '''compose destination directory by replacing the
            source/ part of source_dir with dest/.

            '''
            common = os.path.commonpath([source, source_dir])
            remainder = source_dir[len(common):].strip(os.sep)
            return os.path.join(dest, remainder)

        def _dest_name(source_dir, obj_name):
            '''compose potential destination path to image.

            '''
            return os.path.join(_dest_dir(source_dir), obj_name)

        # cache locations in order to be able to free up the db connection.
        for location_id, source_dir in list(self._collect_locations(source)):

            # Check for all columns refering to a location_id.
            for location_type in MDCStoreHandle.LOCATION_COLUMNS:

                queue = [
                    obj_id
                    for obj_id, obj_name in self.collect_images_at_location(
                        location_id, location_type)
                    if os.path.exists(_dest_name(source_dir, obj_name))
                ]

                self.logger.info('{:>4}:   {}\n\t{} {} in\n\t{}'.format(
                    location_id, source_dir, len(queue), 'files'
                    if location_type == MDCStoreHandle.LOCATION_COLUMNS.OBJ
                    else 'thumbs', _dest_dir(source_dir)))

                if len(queue) >= 1:
                    new_location_id = self._create_new_location(
                        _dest_dir(source_dir))
                    self._update_multiple_files(queue, new_location_id,
                                                location_type)

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
        if not field_type in MDCStoreHandle.LOCATION_COLUMNS:
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
            print('Updated {} items to {}={}'.format(count, field_type.value,
                                                     new_location_id))
            cursor.commit()