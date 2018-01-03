#!/usr/bin/env python
# -*- coding: utf-8 -*-


import psycopg2
import getpass
import traceback
from ezconf import ConfigFile
from psycopg2.extras import DictCursor
from psycopg2.extras import Json
import psycopg2.extensions


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

class DB(object):
    """
    """

    def __init__(
            self,
            host=None, database=None, username=None, password=None,
            args=None, configfile=None):
        self.host = host
        self.database = database
        if username is not None:
            self.username = username
        else:
            self.username = getpass.getuser()
        self.password = password
        self.schema = None
        self.args = args
        self.configfile = configfile

        self.conn = None
        self.cursor = None
        
        self.configure()


    def _override_conf(
            self,
            host=None, database=None, username=None, password=None):
        if host is not None:
            self.host = host

        if database is not None:
            self.database = database

        if username is not None:
            self.username = username

        if password is not None:
            self.password = password

    def configure_ezconf(self, configfile):
        if configfile is not None:
            self._override_conf(
                configfile.getValue("db.host"),
                configfile.getValue("db.database"),
                configfile.getValue("db.username"),
                configfile.getValue("db.password"))


    def configure_args(self, args):
        if self.args is not None:
            self._override_conf(
                args.host,
                args.database,
                args.user)

    def configure(self):
        self.configure_ezconf(self.configfile)
        self.configure_args(self.args)


    def verify_conf(self):
        if self.host is None:
            raise Exception("Database host is required")
        if self.database is None:
            raise Exception("Database name is required")



    def connect(self):
        self.verify_conf()
        try:
            if self.password is None:
                self.password = getpass.getpass(
                    "Enter password for %s@%s (%s) : " % (
                        self.username,
                        self.host,
                        self.database))

            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password
                )
            self.conn.set_client_encoding('utf-8')
        except Exception, e:
            print "failed to connect as '%s@%s' to database '%s'" % (
                self.username,
                self.host,
                self.database)
            traceback.print_exc()
            raise e


    def get_cursor(self):
        if self.conn is None:
            self.connect()

        self.cur = self.conn.cursor(cursor_factory=DictCursor)
        psycopg2.extras.register_default_json(loads=lambda x: x)
        if self.schema is not None and len(self.schema):
            self.cur.execute('SET search_path TO ' + self.schema)

        return self.cur


    def commit(self):
        if self.conn is not None:
            self.conn.commit()
        else:
            print "can't commit. connection is invalid"


    @staticmethod
    def add_args(parser, default_database):
        parser.add_argument(
            '--database', help='database name', default=default_database)
        parser.add_argument('--host', help='host')
        #parser.add_argument('--schema', help='schema name')
        parser.add_argument('-u', '--user', help="username")
        #parser.add_argument('-p', '--password', help="password", action="store_true")


