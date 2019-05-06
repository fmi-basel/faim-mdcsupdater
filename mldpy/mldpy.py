import os
import re
import socket

import pyodbc


def is_subdir(parent, potential_child):
    '''
    '''
    # TODO add realpath again for windows
    parent = os.path.normpath(parent)
    potential_child = os.path.normpath(potential_child)

    return os.path.commonprefix([parent, potential_child]) == parent


def get_host():
    '''
    '''
    return socket.gethostname()


def _split_by_mount(path):
    '''
    '''
    orig_path = path = os.path.realpath(path)
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

    elif re.match('[A-Z]:\\'):
        location['LOCATION_TYPE'] = 1
        location['SERVER_NAME'] = get_host()
        location['SERVER_ROOT'] = mount

    else:
        raise NotImplementedError(
            'Unknown file location type: {}'.format(path))
    return location


class MCDStoreHandle:
    def __init__(self, server, database, username, password):
        '''
        '''
        self.db_conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server}; ' +
            'SERVER={} ; DATABASE={} ; UID={} ; PWD={}'.format(
                server, database, username, password))

    def collect_locations(self, source):
        '''
        '''
        cursor = self.db_conn.cursor()

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

        for location_id, path in cursor.execute(query):

            if path is None:
                continue

            if is_subdir(source, path):
                yield location_id, path

    def collect_images_at_location(self, location_id):
        '''yields all images with the given location_id.

        '''
        query = """
        select
            OBJ_ID, OBJ_SERVER_NAME
        from PLATE_IMAGE_DATA as PI
        where
            LOCATION_ID=?
        """

        cursor = self.db_conn.cursor()

        for plate_image_id, plate_image_name in cursor.execute(
                query, location_id):
            yield plate_image_id, plate_image_name

    def update_file_locations(self, source, dest):
        '''
        '''

        def _source_name(source_dir, obj_name):
            '''
            '''
            return os.path.join(source_dir, obj_name)

        def _dest_dir(source_dir):
            '''
            '''
            common = os.path.commonpath([source, source_dir])
            return os.path.join(dest, source_dir[len(common):])

        def _dest_name(source_dir, obj_name):
            '''
            '''
            return os.path.join(_dest_dir(source_dir), obj_name)

        # cache locations in order to be able to free up the db connection.
        for location_id, source_dir in list(self.collect_locations(source)):

            queue = [
                obj_id for obj_id, obj_name in self.collect_images_at_location(
                    location_id)
                if os.path.exists(_dest_name(source_dir, obj_name))
            ]

            print('{:>4}:   {}\n\t{} files in\n\t{}'.format(
                location_id, source_dir, len(queue), _dest_dir(source_dir)))

            if len(queue) >= 1:
                new_location_id = self._create_new_location(
                    _dest_dir(source_dir))
                self._update_multiple_files(queue, new_location_id)

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

        cursor = self.db_conn.cursor()
        cursor.execute(query, *vals)
        location_id = cursor.fetchval()
        cursor.commit()
        return location_id

    def _get_location_id(self, location):
        '''
        '''
        query = """
        select LOCATION_ID
        from FILE_LOCATION
        where ({})
        """.format(', '.join(
            '{}={}'.format(key, val) for key, val in location.items()))
        cursor = self.db_conn.cursor()
        cursor.execute(query)
        return cursor.fetchval()

    def _update_multiple_files(self, obj_ids, new_location_id):
        '''
        '''
        query = """
        update 
            PLATE_IMAGE_DATA
        set 
            LOCATION_ID={}
        where
            OBJ_ID in ({})
        """.format(new_location_id,
                   ','.join('%d' % obj_id for obj_id in obj_ids))

        cursor = self.db_conn.cursor()
        count = cursor.execute(query)
        print('Updated %d items' % count)
        cursor.commit()


if __name__ == '__main__':

    server = 'localhost'
    database = 'demodb'
    username = 'sa'
    password = 'Abcd1234'

    db_handle = MCDStoreHandle(server, database, username, password)
    db_handle.update_file_locations(source_dir, '/tmp/stuff/and/things/')
