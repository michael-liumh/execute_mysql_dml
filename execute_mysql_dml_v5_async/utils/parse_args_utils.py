# !/usr/bin/env python3
# -*- coding:utf8 -*-
import getpass
import configargparse
import sys
from pathlib import Path
from .other_utils import logger, py_file_path, py_file_pre


def parse_args():
    """parse args to connect MySQL"""

    parser = configargparse.ArgumentParser(
        description='Parse Args', add_help=False,
        formatter_class=configargparse.ArgumentDefaultsHelpFormatter,
        config_file_parser_class=configargparse.YAMLConfigFileParser,
        default_config_files=['conf.d/*.yaml'],  # 可以设置更多路径
    )
    parser.add_argument('--help', dest='help', action='store_true',
                        help='help information', default=False)
    parser.add_argument('-c', '-config', is_config_file=True,
                        help='script config file path')

    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-u', '--user', env_var='MYSQL_USER', dest='user',
                                 type=str, default='root', help='MySQL User')
    connect_setting.add_argument('-p', '--password', env_var='MYSQL_PWD', dest='password',
                                 type=str, nargs='*', default='',
                                 help='MySQL Password')
    connect_setting.add_argument('-h', '--host', env_var='MYSQL_HOST', dest='host',
                                 type=str, default='127.0.0.1', help='MySQL Host')
    connect_setting.add_argument('-P', '--port', env_var='MYSQL_PORT', dest='port',
                                 type=int, default=3306, help='MySQL Port')
    connect_setting.add_argument('-S', '--socket', dest='socket', type=str, default='',
                                 help='MySQL Socket')
    connect_setting.add_argument('-C', '--charset', dest='charset', type=str, default='utf8mb4',
                                 help='MySQL Charset')
    connect_setting.add_argument('--collation', dest='collation', type=str, default='utf8mb4_general_ci',
                                 help='MySQL collation')

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--database', dest='database', type=str, default='',
                        help='Connect MySQL Database')

    sql_file = parser.add_argument_group('sql file')
    sql_file.add_argument('-f', '--file', dest='file_path', type=str, nargs='*', default='',
                          help='SQL file you want to execute')
    sql_file.add_argument('-fd', '--file-dir', dest='file_dir', type=str, default='.',
                          help='SQL file dir')
    sql_file.add_argument('-fr', '--file-regex', dest='file_regex', type=str, default='*.sql',
                          help="SQL file regex, use to find SQL file in file dir. ")
    sql_file.add_argument('-efr', '--exclude-file-regex', dest='exclude_file_regex', type=str,
                          default='executed_.*',
                          help="SQL file exclude regex, use to exclude file you don't want it. ")
    sql_file.add_argument('--start-file', dest='start_file', type=str, default='',
                          help='Start file in SQL file dir')
    sql_file.add_argument('--stop-file', dest='stop_file', type=str, default='',
                          help='Stop file in SQL file dir')
    sql_file.add_argument('--check', dest='check', action='store_true', default=False,
                          help='Check SQL file list if you want')
    sql_file.add_argument('-ma', '--minutes-ago', dest='minutes_ago', type=int, default=1,
                          help='Only files whose last modification time was number minutes ago are executed')

    committed_file = py_file_path.parent / 'logs' / f'committed_{py_file_pre}.json'
    sql_file.add_argument('--save', dest='result_file', type=str, default=committed_file,
                          help='file for save committed parts.')

    execute = parser.add_argument_group('execute method')
    execute.add_argument('--chunk', dest='chunk', type=int, default=2000,
                         help="Execute chunk of line sql in one transaction.")
    execute.add_argument('--interval', dest='interval', type=float, default=0.1,
                         help="Sleep time after execute chunk of line sql. set it to 0 if do not need sleep ")
    execute.add_argument('--reset', dest='reset', action='store_true', default=False,
                         help='Do not ignore committed line')
    execute.add_argument('--file-per-thread', dest='file_per_thread', action='store_true', default=False,
                         help="If set to true, we won't separate file part to execute sql, "
                              "unless you give more than one file. Only one thread per file.")
    execute.add_argument('--threads', dest='threads', type=int, default=1,
                         help="Only execute number of file part at the same time, "
                              "0 means execute all parts at the same time.")
    execute.add_argument('--skip-error-regex', dest='skip_error_regex', type=str,
                         help='specify regex to skip some errors if the regex match the error msg.')
    execute.add_argument('--save-per-commit', dest='save_per_commit', action='store_true', default=False,
                         help='Once commit one part, save it into result file. '
                              'If set to True, the execute time will be much longer.')

    action = parser.add_argument_group('action method')
    action.add_argument('--stop-never', dest='stop_never', action='store_true', default=False,
                        help='Never stop executed file or file in file dir if file increasing')
    action.add_argument('--sleep', dest='sleep', type=int, default=60,
                        help='When you use stop never options, we will sleep specify seconds after '
                             'finished every time.')
    action.add_argument('--delete-file', dest='delete_executed_file', action='store_true', default=False,
                        help='Delete SQL file after executed successfully')
    action.add_argument('--delete-record', dest='delete_not_exists_file_record', action='store_true',
                        help='Delete not exists file record in result file ', default=False)
    return parser


def parse_args_from_command_line(args: configargparse):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)

    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)

    if not args.check:
        if not args.password:
            args.password = getpass.getpass()
        else:
            args.password = args.password[0]

    if args.file_path:
        for f in args.file_path:
            if not Path(f).exists():
                logger.error(f'File {f} does not exists.')
                sys.exit(1)

    if args.file_dir and not Path(args.file_dir).is_dir():
        logger.error(f'File dir {args.file_dir} does not exists.')
        sys.exit(1)

    if args.sleep < 0:
        logger.error(f'Invalid value of sleep')
        sys.exit(1)
    return args
