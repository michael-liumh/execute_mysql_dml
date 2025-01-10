# !/usr/bin/env python3
# -*- coding:utf8 -*-
import sys
import pendulum
from loguru import logger
from pathlib import Path

py_file_path = Path(sys.argv[0])
py_file_pre = py_file_path.parts[-1].replace('.py', '')
log_file = py_file_path.parent / 'logs' / f'{py_file_pre}.log'
log_file.parent.mkdir(exist_ok=True, parents=True)
logger.add(log_file, rotation='100MB', colorize=True, retention=10, compression='zip', enqueue=True)


def ts_now() -> int:
    return int(pendulum.now().timestamp())


def ts_interval(ts1: int = 0, ts2: int = 0):
    ts1 = pendulum.from_timestamp(ts1)
    ts2 = pendulum.from_timestamp(ts2)
    result = pendulum.interval(ts1, ts2, absolute=True).as_duration()
    return result


async def get_log_format(args, sql_file):
    if args.socket:
        info_format = '[{socket}] [{db}] [{file}] '.format(
            socket=args.socket,
            db=args.database,
            file=sql_file
        )
    else:
        info_format = '[{host}] [{port}] [{db}] [{file}] '.format(
            host=args.host,
            port=args.port,
            db=args.database,
            file=sql_file
        )
    finished_info = info_format + 'finished'
    base_format = '[%s] ' % sql_file
    return base_format, info_format, finished_info
