import argparse

from mldpy import MDCStoreHandle


def parse():
    '''
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--dest', required=True)
    parser.add_argument('--host', required=False, default='localhost')
    parser.add_argument('--database', required=False, default='MDCStore')
    parser.add_argument('--username', required=True)
    parser.add_argument('-pw', '--password', required=True)

    return parser.parse_args()


def main():
    '''
    '''
    args = parse()

    with MDCStoreHandle(host=args.host,
                        database=args.database,
                        username=args.username,
                        password=args.password) as db_handle:
        db_handle.update_file_locations(args.source, args.dest)


if __name__ == '__main__':
    main()
