from __future__ import print_function, unicode_literals

import argparse
import logging
import os
import re
import signal
import sys
import time

from pymydump.dumper import MySQLDumper
from pymydump.errors import PyMyDumpError
from pymydump.expire import ExpireDirectoryNumFiles
from pymydump.log import set_debug, setup_logging
from pymydump.output import FileOutput
from pymydump.stream import DBStream

DEFAULT_DB_PATTERN = r'^(?!(information_schema|performance_schema|sys)$)'


def run_tool(args):
    if not args.out_file and not args.out_dir:
        args.out_file = '-'
    if args.out_file and args.out_dir:
        raise PyMyDumpError('cannot have both out_file and out_dir')

    dumper = MySQLDumper(
        host=args.host,
        username=args.username,
        password=args.password,
        opts=args.mysqldump_opts)

    single_stream = True if args.out_file else False
    stream = DBStream(
        dumper,
        pattern=args.db_pattern,
        compressor_name=args.compress,
        single_stream=single_stream)

    out = FileOutput(stream.stream())
    if args.out_file:
        out.write_to_file(args.out_file)
    if args.out_dir:
        type_suffix = '.sql'
        if args.compress == 'bz2':
            type_suffix += '.bz2'
        if args.keep > 0:
            expire = ExpireDirectoryNumFiles(args.out_dir, args.keep)
        suffix = '-{}{}'.format(time.strftime('%Y%m%d%H%M%S'), type_suffix)
        for name, db in out.write_to_dir(args.out_dir, suffix):
            print(name)
            if args.keep > 0:
                expire_pat = re.compile(r'^{}-[0-9]+{}$'.\
                    format(db, type_suffix))
                expire.expire(expire_pat)


def main():
    setup_logging()
    parser = argparse.ArgumentParser(
        description='Tool to do sensible MySQL dumps with mysqldump')
    parser.add_argument(
        '--keep',
        type=int,
        metavar='NUM',
        default=os.environ.get('PYMYDUMP_KEEP', -1),
        help='Keep num amount of dumps, makes only sense with --outdir')
    parser.add_argument(
        '--username',
        metavar='STRING',
        default=os.environ.get('PYMYDUMP_USERNAME', os.environ.get('USER')),
        help='Username to use to connect to database')
    parser.add_argument(
        '--compress',
        choices=['none', 'bz2'],
        default=os.environ.get('PYMYDUMP_COMPRESS', 'none'),
        help='Dump compression method')
    parser.add_argument(
        '--password',
        metavar='STRING',
        default=os.environ.get('PYMYDUMP_PASSWORD'),
        help='Password to use to connect to database')
    parser.add_argument(
        '--host',
        metavar='HOSTNAME',
        default=os.environ.get('PYMYDUMP_HOST', 'localhost'),
        help='Host to connect to')
    parser.add_argument(
        '--db-pattern',
        metavar='REGEXP',
        type=re.compile,
        default=os.environ.get('PYMYDUMP_DB_PATTERN', DEFAULT_DB_PATTERN),
        help='Databases to be dumped')
    parser.add_argument(
        '--mysqldump-opts',
        metavar='KEY1=VAL,KEY2=VAL,...',
        default=os.environ.get('PYMYDUMP_MYSQLDUMP_OPTS'),
        help='Additional options to pass to mysqldump')
    parser.add_argument(
        '--out-file',
        metavar='FILE',
        default=os.environ.get('PYMYDUMP_OUTFILE'),
        help='File to write dumps to. Use - for stdout')
    parser.add_argument(
        '--out-dir',
        metavar='PATH',
        default=os.environ.get('PYMYDUMP_OUTDIR'),
        help='Path to write dumps in individual files')
    parser.add_argument(
        '--debug',
        action='store_true',
        default=parse_bool(os.environ.get('PYMYDUMP_DEBUG')),
        help='Enable debug logging to STDERR')

    args = parser.parse_args()

    try:
        if args.debug:
            set_debug()
        if args.mysqldump_opts:
            props = args.mysqldump_opts[:]
            args.mysqldump_opts = [parse_kvs(item)
                                   for item in parse_list(props)]
        run_tool(args)
    except PyMyDumpError as e:
        print('ERROR: {}'.format(e), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('User interrupt')
        return 1
    return 0


def parse_bool(val):
    if val and val.lower() in ['true', 't', '1']:
        return True
    else:
        return False


def parse_list(val):
    if val:
        return val.split(',')
    else:
        return []


def parse_kvs(val):
    p = val.split('=')
    if len(p) == 1:
        return (p[0].strip(), None)
    elif len(p) == 2:
        return (p[0].strip(), p[1].strip())
    else:
        raise PyMyDumpError('cannot parse: {}'.format(val))


if __name__ == '__main__':
    sys.exit(main())
