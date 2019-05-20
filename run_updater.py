import argparse
import configparser

from mldpy import MDCStoreHandle


def parse():
    '''
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--dest', required=True)

    mutex_group = parser.add_mutually_exclusive_group()
    spec_group = mutex_group.add_argument_group()
    spec_group.add_argument('--host', required=False, default='localhost')
    spec_group.add_argument('--database', required=False, default='MDCStore')
    username_arg = spec_group.add_argument('--username', required=False)
    spec_group.add_argument('-pw', '--password', required=False)

    mutex_group.add_argument('--config', required=False)

    args = parser.parse_args()

    if args.config is not None:
        config = configparser.ConfigParser()
        config.read(args.config)
        for key, val in ((key, val) for section in config.sections()
                         for key, val in config.items(section=section)):
            if key in ['host', 'database', 'username', 'password']:
                setattr(args, key, val)

    if args.username is None or args.password is None:
        raise argparse.ArgumentError(username_arg, 
            'Both --username and --password must be set or defined '
            'in the given config.')
    return args


def main():
    '''
    '''
    args = parse()

    print(args)

    with MDCStoreHandle(
            host=args.host,
            database=args.database,
            username=args.username,
            password=args.password) as db_handle:
        db_handle.update_file_locations(args.source, args.dest)


if __name__ == '__main__':
    main()
