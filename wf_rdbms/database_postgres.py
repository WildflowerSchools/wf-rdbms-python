from wf_rdbms.database import Database
import psycopg2
import psycopg2.sql
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DatabasePostgres(Database):
    """
    Class to define a Postgres database implementation
    """
    def __init__(
        self,
        name,
        tables,
        user=None,
        password=None,
        host=None,
        port=None
    ):
        super().__init__(name, tables)
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cur = None

    def connect(self, connect_to_database=True):
        if self.conn is not None:
            raise ValueError('Database already connected')
            return self.conn
        if connect_to_database:
            self.conn = psycopg2.connect(
                dbname=self.name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        else:
            self.conn = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        return self.conn

    def open_cursor(self, cursor_factory=None):
        if self.cur is not None:
            raise ValueError('Cursor already generated')
            return self.cur
        if self.conn is None:
            self.connect()
        self.cur = self.conn.cursor(cursor_factory=cursor_factory)
        return self.cur

    def close_connection(self):
        if self.conn is None:
            raise ValueError('No connection open')
            return
        self.conn.close()
        self.conn = None

    def close_cursor(self):
        if self.cur is None:
            raise ValueError('No cursor open')
            return
        self.cur.close()
        self.cur = None

    def initialize_database(self):
        self.create_database()
        for table in self.tables.values():
            self.create_table(table)

    def create_database(self):
        if self.database_exists():
            raise ValueError('Database {} already exists'.format(self.name))
        else:
            logger.info('Database {} does not exist. Creating'.format(self.name))
        self.connect(connect_to_database=False)
        self.open_cursor()
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        sql_string = psycopg2.sql.SQL('CREATE DATABASE {};').format(psycopg2.sql.Identifier(self.name))
        self.cur.execute(sql_string)
        self.close_cursor()
        self.close_connection()
        if self.database_exists():
            logger.info('Database {} successfully created'.format(self.name))
        else:
            raise ValueError('Failed to create database {}'.format(self.name))

    def database_exists(self):
        self.connect(connect_to_database=False)
        self.open_cursor()
        sql_string = psycopg2.sql.SQL('SELECT datname from pg_database;')
        self.cur.execute(sql_string)
        databases = self.cur.fetchall()
        self.close_cursor()
        self.close_connection()
        return (self.name,) in databases

    def create_table(self, table):
        if self.table_exists(table):
            raise ValueError('Table {} already exists'.format(table.name))
        else:
            logger.info('Table {} does not exist. Creating'.format(table.name))
        self.connect()
        self.open_cursor()
        sql_string = self.create_table_sql_string(table)
        logger.info('Sending SQL string:\n{}'.format(sql_string.as_string(self.cur)))
        self.cur.execute(sql_string)
        self.conn.commit()
        self.close_cursor()
        self.close_connection()
        if self.table_exists(table):
            logger.info('Successfully created table {}'.format(table.name))
        else:
            raise ValueError('Failed to create table {}'.format(table.name))

    def table_exists(self, table):
        self.connect()
        self.open_cursor()
        sql_string = psycopg2.sql.SQL('SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name={})').format(
            psycopg2.sql.Placeholder(name='table_name')
        )
        self.cur.execute(sql_string, {'table_name': table.name})
        exists = self.cur.fetchone()[0]
        self.close_cursor()
        self.close_connection()
        return exists

    def create_table_sql_string(self, table):
        argument_list = [self.create_field_sql_string(field) for field in table.fields.values()]
        argument_list.append(
            psycopg2.sql.SQL('PRIMARY KEY({})').format(
                psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(field_name) for field_name in table.primary_key])
            )
        )
        if table.foreign_keys is not None:
            for foreign_key in table.foreign_keys:
                argument_list.append(
                    psycopg2.sql.SQL('FOREIGN KEY ({}) REFERENCES {}').format(
                        psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(field_name) for field_name in foreign_key[1]]),
                        psycopg2.sql.Identifier(foreign_key[0])
                    )
                )
        sql_string = psycopg2.sql.SQL('CREATE TABLE {} ({});').format(
            psycopg2.sql.Identifier(table.name),
            psycopg2.sql.SQL(', ').join(argument_list)
        )
        return sql_string

    def create_field_sql_string(self, field):
        argument_list = [
            psycopg2.sql.Identifier(field.name),
            psycopg2.sql.SQL(field.type._sql_type)
        ]
        if field.unique:
            argument_list.append('UNIQUE')
        if field.not_null:
            argument_list.append('NOT NULL')
        sql_string = psycopg2.sql.SQL(' ').join(argument_list)
        return sql_string

    def create_records_from_dataframe(
        self,
        table_name,
        dataframe
    ):
        field_names = self.tables[table_name].field_names
        dataframe = dataframe.reset_index()
        records = dataframe.to_dict(orient='records')
        self.create_records_from_dict_list(
            table_name=table_name,
            records=records
        )

    def create_records_from_dict_list(
        self,
        table_name,
        records
    ):
        converted_records, included_fields = self.normalize_records_dict_list(
            table_name,
            records
        )
        included_fields = list(included_fields)
        field_names = self.tables[table_name].field_names
        sql_string = psycopg2.sql.SQL('INSERT INTO {} ({}) VALUES {}').format(
            psycopg2.sql.Identifier(table_name),
            psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(field_name) for field_name in included_fields]),
            psycopg2.sql.Placeholder()
        )
        template = psycopg2.sql.SQL('({})').format(
            psycopg2.sql.SQL(', ').join(
                [psycopg2.sql.Placeholder(field_name) for field_name in included_fields]
            )
        )
        self.connect()
        self.open_cursor()
        psycopg2.extras.execute_values(
            cur=self.cur,
            sql=sql_string,
            argslist=converted_records,
            template=template
        )
        self.conn.commit()
        self.close_cursor()
        self.close_connection()

    def normalize_records_dict_list(
        self,
        table_name,
        records
    ):
        field_names = self.tables[table_name].field_names
        primary_key = self.tables[table_name].primary_key
        converted_records = list()
        included_fields = set()
        for index, record in enumerate(records):
            for primary_key_field in primary_key:
                if primary_key_field not in record.keys():
                    raise ValueError('Record {} does not contain primary key field: {}'.format(
                        index,
                        primary_key_field
                    ))
            converted_record = dict()
            for key, value in record.items():
                if key in field_names:
                    included_fields.add(key)
                    converted_record[key] = self.tables[table_name].fields[key].type.to_python_object(record[key])
            converted_records.append(converted_record)
        return converted_records, included_fields

    def fetch_records_as_dataframe(
        self,
        table_name,
        requested_field_names=None
    ):
        records = self.fetch_records_as_dict_list(
            table_name,
            requested_field_names=requested_field_names
        )
        if requested_field_names is None:
            requested_field_names = self.tables[table_name].field_names
        dtypes = dict([(requested_field_name, self.tables[table_name].fields[requested_field_name].type._pandas_dtype) for requested_field_name in requested_field_names])
        df = pd.DataFrame.from_records(records).astype(dtypes)
        return df

    def fetch_records_as_dict_list(
        self,
        table_name,
        requested_field_names=None
    ):
        field_names = self.tables[table_name].field_names
        if requested_field_names is not None:
            for requested_field_name in requested_field_names:
                if requested_field_name not in field_names:
                    raise ValueError('Requested field \'{}\' not in table'.format(requested_field_name))
            select_sql_string = psycopg2.sql.SQL('{}').format(
                psycopg2.sql.SQL(', ').join(
                    [psycopg2.sql.Identifier(requested_field_name) for requested_field_name in requested_field_names]
                )
            )
        else:
            requested_field_names=field_names
            select_sql_string = psycopg2.sql.SQL('*')
        sql_string = psycopg2.sql.SQL('SELECT {} from {}').format(
            select_sql_string,
            psycopg2.sql.Identifier(table_name)
        )
        self.connect()
        self.open_cursor()
        self.cur.execute(sql_string)
        results=self.cur.fetchall()
        self.close_cursor()
        self.close_connection()
        records = [dict(zip(requested_field_names, result)) for result in results]
        return records
