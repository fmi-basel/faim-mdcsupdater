import argparse
import configparser
import os
import logging

from mldpy import MDCStoreHandle

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] (%(name)s) [%(levelname)s]: %(message)s',
    datefmt='%d.%m.%Y %H:%M:%S')


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


def parse():
    '''
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--dest', required=True)

    parser.add_argument('--host', required=False)
    parser.add_argument('--database', required=False)
    parser.add_argument('--username', required=False)
    parser.add_argument('--driver', required=False)
    parser.add_argument('-pw', '--password', required=False)

    parser.add_argument('--config', required=False, default=CONFIG_FNAME)
    parser.add_argument('-v', '--verbose', default=False, action='store_true')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        config = get_config(args.config)
        for key, val in config.items():
            if hasattr(args, key) and getattr(args, key) is None:
                setattr(args, key, val)
    except RuntimeError:
        logging.getLogger(__name__).warning('Could not read config from %s',
                                            args.config)

    if args.username is None or args.password is None:
        raise argparse.ArgumentError(
            'Both --username and --password must be set or defined '
            'in the given config.')
    return args


def main():
    '''
    '''
    args = parse()

    with MDCStoreHandle(host=args.host,
                        database=args.database,
                        username=args.username,
                        password=args.password,
                        driver=args.driver) as db_handle:
        db_handle.update_file_locations(args.source, args.dest)


if __name__ == '__main__':
    main()
