#!/usr/bin/env python3

# All actions with DB are here

from database_error import DatabaseError

import psycopg2
from psycopg2.extensions import cursor

def _make_select_part_with_as(fields: dict) -> str:
    select_part = ''

    for field, value in fields.items():
        if len(select_part) != 0:
            select_part += ', '
        select_part += "{} as {}".format(field, value)

    return select_part

def _make_where_part(wheres: dict) -> str:
    where_part = ''

    for field, value in wheres.items():
        if len(where_part) != 0:
            where_part += ' and '
        if value is None:
            where_part += "{} is NULL".format(field)
        elif isinstance(value, dict):
            where_part += "{} {} {}".format(field, value['sign'], value['value'])
        elif isinstance(value, list):
            where_part += "{} in ({})".format(field, ','.join(value))
        else:
            where_part += "{} = {}".format(field, value)

    return where_part

def _make_set_part(data: dict) -> str:
    set_part = ''

    for field, value in data.items():
        if len(set_part) != 0:
            set_part += ', '
        if value is None:
            set_part += "{} = NULL".format(field)
        else:
            set_part += "{} = {}".format(field, value)

    return set_part

def _is_table_exist(cur: cursor, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (name,))
    db_name = cur.fetchone()
    if len(db_name) == 0:
        return False
    return db_name[0] == name

# !!! make universal maker tables and call it from API
def _create_table_users(cur: cursor) -> None:
    # id equals telegram user id
    cur.execute(
        '''
            CREATE TABLE users (
                id BIGINT PRIMARY KEY,
                nick VARCHAR (255) UNIQUE NOT NULL,
                fullname VARCHAR (255) NOT NULL
            )
        '''
    )

def _create_table_pools(cur: cursor) -> None:
    cur.execute(
        '''
            CREATE TABLE pools (
                id serial PRIMARY KEY,
                name_ru VARCHAR (255) UNIQUE NOT NULL
            )
        '''
    )

def _create_table_sessions(cur: cursor) -> None:
    cur.execute(
        '''
            CREATE TABLE sessions (
                id serial PRIMARY KEY,
                pool_id INT NOT NULL REFERENCES pools (id),
                time VARCHAR (5) NOT NULL,
                weekday VARCHAR (1) NOT NULL,
                group_type VARCHAR (50)
            )
        '''
    )

def _create_table_buy_records(cur: cursor) -> None:
    cur.execute(
        '''
            CREATE TABLE buy_records (
                id serial PRIMARY KEY,
                record_time TIMESTAMP NOT NULL DEFAULT NOW(),
                user_id BIGINT NOT NULL REFERENCES users (id),
                canceled BOOLEAN NOT NULL DEFAULT FALSE,
                cancel_time TIMESTAMP
            )
        '''
    )

def _create_table_sell_records(cur: cursor) -> None:
    cur.execute(
        '''
            CREATE TABLE sell_records (
                id serial PRIMARY KEY,
                record_time TIMESTAMP NOT NULL DEFAULT NOW(),
                user_id BIGINT NOT NULL REFERENCES users (id),
                session_id INT NOT NULL REFERENCES sessions (id),
                trade_in_date VARCHAR (10) NOT NULL,
                buy_id INT REFERENCES buy_records (id),
                canceled BOOLEAN NOT NULL DEFAULT FALSE,
                cancel_time TIMESTAMP
            )
        '''
    )

# !!! Обезопасить запросы в БД
class DatabaseInternal():
    def __init__(self, url: str):
        self.url = url
        con = None
        try:
            con = psycopg2.connect(self.url, sslmode='require')
            #con = psycopg2.connect(self.url)
            cur = con.cursor()
            if not _is_table_exist(cur = cur, name = 'users'):
                _create_table_users(cur)
            if not _is_table_exist(cur = cur, name = 'pools'):
                _create_table_pools(cur)
            if not _is_table_exist(cur = cur, name = 'sessions'):
                _create_table_sessions(cur)
            if not _is_table_exist(cur = cur, name = 'buy_records'):
                _create_table_buy_records(cur)
            if not _is_table_exist(cur = cur, name = 'sell_records'):
                _create_table_sell_records(cur)
            con.commit()
            cur.close()
        except Exception as error:
            print('Error with request to database. Cause: {}'.format(error)) # !!! use logger
        finally:
            if con is not None:
                con.close()

    def select(self, get_fields, table: str, wheres: dict = {}, joins: list = []) -> (DatabaseError, list):
        result = []
        status = DatabaseError.Ok
        con = None
        try:
            con = psycopg2.connect(self.url)
            cur = con.cursor()

            fields = ', '.join(get_fields) if isinstance(get_fields, list) else _make_select_part_with_as(get_fields)
            request = 'SELECT {} FROM {}'.format(fields, table)

            for join in joins:
                request += ' INNER JOIN {} ON {}'.format(join['table'], _make_where_part(join['on']))
            
            if len(wheres) != 0:
                request += ' WHERE ' + _make_where_part(wheres)

            cur.execute(request)
            all_rows = cur.fetchall()

            cur.close()

            column_names = get_fields if isinstance(get_fields, list) else list(get_fields.values())
            for row in all_rows:
                if len(row) == len(column_names):
                    data = {}
                    for i in range(len(row)):
                        data[column_names[i]] = None if row[i] is None else str(row[i])
                    result.append(data)
        except Exception as error:
            print('Could not connect to the Database.') # !!! use logger
            print('Cause: {}'.format(error)) # !!!
            status = DatabaseError.InternalError
        finally:
            if con is not None:
                con.close()
            return status, result

    def record_exists(self, table: str, wheres: dict) -> (DatabaseError, bool):
        status, db_record_info = self.select(['1'], table, wheres)

        if status != DatabaseError.Ok or len(db_record_info) == 0:
            return status, False

        return status, True


    def insert_with_ret(self, table: str, data: dict, ret: list) -> (DatabaseError, dict):
        status = DatabaseError.Ok
        returning = {}
        con = None
        try:
            con = psycopg2.connect(self.url)
            cur = con.cursor()

            columns = ', '.join(data.keys())
            values = ', '.join(map(str, data.values()))
            text = 'INSERT INTO {} ({}) VALUES ({})'.format(table, columns, values)
            if len(ret) != 0:
                text += ' RETURNING ' + ', '.join(ret)

            cur.execute(text)

            row = []
            if len(ret) != 0:
                row = cur.fetchone()

            con.commit()
            cur.close()

            if len(row) != 0:
                for i in range(len(ret)):
                    returning[ret[i]] = None if row[i] is None else str(row[i])

        except Exception as error:
            print('Could not connect to the Database.') # !!! use logger
            print('Cause: {}'.format(error)) # !!!
            status = DatabaseError.InternalError
        finally:
            if con is not None:
                con.close()
            return status, returning

    def insert(self, table: str, data: dict) -> DatabaseError:
        status, _ = self.insert_with_ret(table, data, [])
        return status


    def update(self, table: str, data: dict, wheres: dict) -> DatabaseError:
        status = DatabaseError.Ok
        con = None
        try:
            con = psycopg2.connect(self.url)
            cur = con.cursor()

            cur.execute('UPDATE {} SET {} WHERE ({})'.format(table, _make_set_part(data), _make_where_part(wheres)))

            con.commit()
            cur.close()
        except Exception as error:
            print('Could not connect to the Database.') # !!! use logger
            print('Cause: {}'.format(error)) # !!!
            status = DatabaseError.InternalError
        finally:
            if con is not None:
                con.close()
            return status
