# !/usr/bin/env python3
# -*- coding:utf8 -*-
import os
import json
from pathlib import Path
from .other_utils import ts_now, logger


def get_sql_file_list(args):
    file_list = []
    if args.file_dir and not args.file_path:
        for current_dir, sub_dir, files in Path(args.file_dir).walk():
            for f in files:
                f = Path(f)
                if args.start_file and f.name < args.start_file:
                    continue
                if args.stop_file and f.name > args.stop_file:
                    break
                if (args.file_regex and f.match(args.file_regex) is False) or \
                        (args.exclude_file_regex and f.match(args.exclude_file_regex)):
                    continue

                sql_file = Path(current_dir) / f
                if int(ts_now() - Path(sql_file).stat().st_mtime) < args.minutes_ago * 60:
                    continue
                file_list.append(sql_file)
    else:
        for f in args.file_path:
            f = Path(f)
            if (args.file_regex and f.match(args.file_regex)) and \
                    (args.exclude_file_regex and f.match(args.exclude_file_regex) is False):
                sql_file = f.absolute()
                if int(ts_now() - sql_file.stat().st_mtime) < args.minutes_ago * 60:
                    continue
                file_list.append(sql_file)
    file_list.sort()
    logger.info(f'Total file count: {len(file_list)}')
    return file_list


def read_file(filename):
    with Path(filename).open() as f:
        return json.loads(f.read())


def get_file_record_part_start_end(part):
    start_part = []
    end_part = []
    for value in part:
        if '-' in value:
            value_split = value.split('-')
            start_line = value_split[0]
            end_line = value_split[1]
        else:
            start_line = end_line = value
        start_part.append(start_line)
        end_part.append(end_line)
    return start_part, end_part


def get_file_executed_record(args, sql_file):
    executed_result = read_file(args.result_file) if Path(args.result_file).exists() else {}
    committed_part = executed_result.get(sql_file, [])

    if args.reset:
        committed_part = []
        committed_part_start = []
        committed_part_end = []
    else:
        committed_part_start, committed_part_end = get_file_record_part_start_end(committed_part)
    return committed_part, committed_part_start, committed_part_end


def save_executed_result(result_file, sql_file, committed_part, delete_not_exists_file_record=False,
                         executed_all_parts=False):
    sql_file = str(sql_file)
    executed_result = read_file(result_file) if Path(result_file).exists() else {}
    executed_result[sql_file] = committed_part
    if delete_not_exists_file_record and executed_all_parts:
        for f in executed_result.copy().keys():
            if not Path(f).exists():
                del executed_result[f]
    msg = json.dumps(executed_result, ensure_ascii=False, indent=4) + '\n'
    with open(result_file, 'w', encoding='utf8') as f:
        f.write(msg)
    return


def check_line_whether_executable(line, line_index, base_format, ignore_part_start, ignore_part_end,
                                  ignore_line_idx_list):
    for part_start, part_end in zip(ignore_part_start, ignore_part_end):
        if int(part_start) <= line_index <= int(part_end):
            return False

    if line == '':
        logger.warning(base_format + '[Ignore null content line: %s] %s' % (line_index, line))
        ignore_line_idx_list.append(line_index)
        return False

    sql_type = line.strip()[:7].strip().upper()
    if sql_type not in ['INSERT', 'UPDATE', 'DELETE', 'REPLACE']:
        logger.warning(base_format + '[Ignore line: %s] %s' % (line_index, line))
        ignore_line_idx_list.append(line_index)
        return False
    return True


def modify_idx_record_list(idx_record_list):
    tmp_list = []
    start_idx = 0
    last_idx = 0
    for i, idx_record in enumerate(idx_record_list):
        if i == 0:
            if isinstance(idx_record, int):
                start_idx = idx_record
                last_idx = idx_record
            elif isinstance(idx_record, str):
                idx_record_split = idx_record.split('-')
                start_idx = int(idx_record_split[0])
                last_idx = int(idx_record_split[-1])
            else:
                logger.error(idx_record)
            continue

        if isinstance(idx_record, int):
            if abs(idx_record - last_idx) != 1:
                tmp_record = f'{start_idx}-{last_idx}'
                tmp_list.append(tmp_record)
                start_idx = idx_record
            last_idx = idx_record
        elif isinstance(idx_record, str):
            idx_record_split = idx_record.split('-')
            idx_record_start = int(idx_record_split[0])
            idx_record_end = int(idx_record_split[-1])
            if abs(idx_record_start - last_idx) != 1:
                tmp_record = f'{start_idx}-{last_idx}'
                tmp_list.append(tmp_record)
                start_idx = idx_record_start
            last_idx = idx_record_end
        else:
            logger.error(f'{idx_record} is not index or index range')
    else:
        if idx_record_list:
            tmp_record = f'{start_idx}-{last_idx}'
            tmp_list.append(tmp_record)
    return list(set(tmp_list))


def sort_start(key):
    if isinstance(key, str):
        key_split = key.split('-')
        return int(key_split[0])
    else:
        return key


def file_handle(filename, base_format, committed_part, ignore_part_start, ignore_part_end, args):
    sql_list = []  # SQL 列表：用于保存可执行的 SQL
    sql_idx_list = []  # SQL 行数列表：用于保存可执行的 SQL 在原文件中的行数，报错时能准确知道错误 SQL 的所在行
    ignore_line_idx_list = []  # 被跳过的行数列表

    if committed_part and not args.stop_never and not args.reset:
        file_lines = "1-" + str(os.popen("wc -l " + filename + " | awk '{print $1}'").read().strip())
        if committed_part[0] == file_lines:
            logger.warning(f'File {filename} had been executed all line parts, skip it.')
            return sql_list, sql_idx_list
        else:
            logger.warning(base_format + 'Ignore committed line parts: %s' % committed_part)

    with open(filename, 'r', encoding='utf8') as fh:
        for idx, line in enumerate(fh):
            idx = idx + 1
            line = line.strip().replace('\n', '')

            executable = check_line_whether_executable(
                line, idx, base_format, ignore_part_start, ignore_part_end, ignore_line_idx_list
            )
            if not executable:
                continue

            sql_list.append(line)
            sql_idx_list.append(idx)

            if idx != 0 and idx % args.chunk == 0:
                yield sql_list, sql_idx_list
                sql_list = []
                sql_idx_list = []
        else:
            if sql_list != [] and sql_idx_list != []:
                yield sql_list, sql_idx_list
                sql_list = []

            yield sql_list, ignore_line_idx_list
