import argparse

from mldpy import MCDStoreHandle


def parse():
    '''
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--dest', required=True)
    parser.add_argument('--host', required=False, default='localhost')
    parser.add_argument('--database', required=False, default='MDCStore')
    parser.add_argument('--username', required=True)
    parser.add_argument('-pw','--password', required=True)

    return parser.parse_args()


def main():
    '''
    '''
    args = parse()

    db_handle = MCDStoreHandle(args.host, args.database, args.username,
                               args.password)
    db_handle.update_file_locations(args.source, args.dest)


if __name__ == '__main__':
    main()
