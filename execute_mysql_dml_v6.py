# !/usr/bin/env python3
# -*- coding:utf8 -*-
import re
import sys
import time
from copy import deepcopy
from pathlib import Path
from utils.mysql_utils import MySQLUtils
from utils.file_utils import modify_idx_record_list, sort_start, save_executed_result, \
    get_file_executed_record, file_handle, get_sql_file_list
from utils.parse_args_utils import parse_args_from_command_line
from utils.other_utils import logger, get_log_format, ts_now, ts_interval


def execute_sql(cursor, sql_list, sql_idx_list, args, base_format, info_format):
    is_finished = False
    affected_rows = 0
    sql_idx = 0
    sql = ''

    try:
        for sql, sql_idx in zip(sql_list, sql_idx_list):
            try:
                cursor.execute(sql)
            except Exception as e:
                if args.skip_error_regex and re.search(args.skip_error_regex, str(e)) is not None:
                    pass
                else:
                    raise e
            affected_rows += cursor.rowcount
        else:

            cursor.execute('commit')
            committed_line_range = ",".join(modify_idx_record_list(sql_idx_list))
            logger.info(info_format + f'[Committed line range: {committed_line_range}] '
                                      f'[Affected rows: {affected_rows}]')
            is_finished = True
    except Exception as e:
        cursor.execute('rollback')
        logger.exception(base_format + str(e))
        err_msg = base_format + '[Error line: %s] %s' % (sql_idx, sql)
        logger.error(err_msg)
        sys.exit(1)

    return is_finished, sql_idx_list


def execute_task(task, committed_part, unfinished_line_parts, args, sql_file):
    is_finished, sql_idx_list = task
    if is_finished:
        committed_part += sql_idx_list
        if args.save_per_commit:
            committed_part.sort(key=sort_start)
            save_executed_result(args.result_file, sql_file, modify_idx_record_list(committed_part))
    else:
        unfinished_line_parts.extend(modify_idx_record_list(sql_idx_list))
    return True


def execute_sql_from_file(args, sql_file, cursor):
    if not Path(sql_file).exists():
        logger.error(f'File {sql_file} does not exists.')
        return False

    logger.info(f'Execute commands from file [{sql_file}]')
    base_format, info_format, finished_info = get_log_format(args, sql_file)
    committed_part, committed_part_start, committed_part_end = get_file_executed_record(args, sql_file)
    unfinished_line_parts = []
    executed_all_parts = False

    try:
        for i, (sql_list, sql_idx_list) in enumerate(file_handle(sql_file, base_format, committed_part,
                                                                 deepcopy(committed_part_start),
                                                                 deepcopy(committed_part_end), args)):
            if sql_list:
                task = execute_sql(
                    cursor, sql_list, sql_idx_list, args, base_format, info_format
                )
                execute_task(task, committed_part, unfinished_line_parts, args, sql_file)
                time.sleep(args.interval)
            else:
                committed_part += sql_idx_list
        else:
            if unfinished_line_parts:
                logger.error(info_format + f'Not all tasks finished, unfinished line parts: '
                                           f'[{",".join(unfinished_line_parts)}]')
            else:
                executed_all_parts = True
                logger.info(finished_info)
                if args.delete_executed_file and int(ts_now() - Path(sql_file).stat().st_mtime) > 60:
                    Path(sql_file).unlink()
    finally:
        committed_part.sort(key=sort_start)
        committed_part = modify_idx_record_list(committed_part)
        save_executed_result(
            args.result_file, sql_file, committed_part, args.delete_not_exists_file_record,
            executed_all_parts
        )
    return True


def main(args, execute_file_list):
    ts_start = ts_now()
    mysql_obj = MySQLUtils(
        host=args.host, port=args.port, socket=args.socket, user=args.user, password=args.password,
        database=args.database, charset=args.charset, collation=args.collation
    )
    try:
        mysql_obj.connect2mysql()

        if not get_sql_file_list:
            execute_file_list = get_sql_file_list(args)

        while True:
            for sql_file in execute_file_list:
                execute_sql_from_file(args, sql_file, mysql_obj.cursor)

            if not args.stop_never:
                break
            time.sleep(args.sleep)
            execute_file_list = get_sql_file_list(args)
    finally:
        mysql_obj.close()
        logger.info('Total used time: %s' % (ts_interval(ts_now(), ts_start)))
    return


if __name__ == "__main__":
    command_line_args = parse_args_from_command_line(sys.argv[1:])

    sql_file_list = get_sql_file_list(command_line_args)
    if command_line_args.check:
        from pprint import pprint

        pprint(sql_file_list)
        sys.exit(1)

    if not sql_file_list:
        logger.error(f'No sql files select or the sql files whose last modification time is '
                     f'less than {command_line_args.minutes_ago} minutes. Or you can add -ma 0 options. '
                     f'Or the file is not match file regex {command_line_args.file_regex}')
        if not command_line_args.stop_never:
            sys.exit(1)

    assert command_line_args.database, "No database select."
    main(command_line_args, sql_file_list)
