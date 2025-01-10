# !/usr/bin/env python3
# -*- coding:utf8 -*-

# pip3 install mysql-connector-python
import mysql.connector as cpy


class MySQLUtils(object):
    def __init__(
            self, host: str = 'localhost', port: int = 3306, socket: str = '',
            user: str = 'root',  password: str = '', database: str = '',
            charset: str = 'utf8mb4',  collation: str = 'utf8mb4_general_ci',
            autocommit: bool = False, pool_size: int = None
    ):
        if not database:
            raise ValueError('Lack of parameter: database')

        self.host = host
        self.port = port
        self.socket = socket
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self.collation = collation
        self.autocommit = autocommit
        self.pool_size = pool_size

        self.conn_setting = {
            "host": self.host, "port": self.port, "unix_socket": self.socket,
            "user": self.user, "password": self.password, "database": self.database,
            "charset": self.charset, "collation": self.collation,
            "autocommit": self.autocommit
        }
        if self.pool_size:
            self.conn_setting['pool_size'] = pool_size

        self.connection = None
        self.cursor = None

    def connect2mysql(self):
        """兼具单连接和连接池功能"""
        self.connection = cpy.connect(**self.conn_setting)
        self.cursor = self.connection.cursor(dictionary=True)
        return

    def execute_sql(self, sql, params: tuple = None):
        """
        将参数以元组的形式传入，如：
        await self.cursor.execute("SELECT `id`, `password` FROM `auth_user` WHERE `id`=%s ", (userid,))
        :return:
        """
        if self.connection is None:
            self.connect2mysql()
        if params is None:
            params = tuple()

        self.cursor.execute(sql, params)
        return

    def close(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.connection is not None:
            self.connection.close()
        return

    def __exit__(self):
        self.close()

