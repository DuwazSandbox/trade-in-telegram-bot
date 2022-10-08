#!/usr/bin/env python3

# All actions with DB are here

from database.error import DatabaseError

from enum import Enum
import logging
import psycopg2
from psycopg2.extensions import cursor

logger = logging.getLogger(__name__)

def _make_select_part_with_as(fields: dict) -> str:
    select_part = ''

    for field, value in fields.items():
        if len(select_part) != 0:
            select_part += ', '
        select_part += "{} as {}".format(field, value)

    return select_part

def _make_where_part(wheres: dict) -> (str, list):
    where_part = ''
    where_args = []

    for field, value in wheres.items():
        if len(where_part) != 0:
            where_part += ' and '
        if value is None:
            where_part += '{} is NULL'.format(field)
        elif isinstance(value, dict):
            where_part += '{} {} '.format(field, value['sign'])
            if isinstance(value['value'], str):
                where_part += '%s'
                where_args.extend([value['value']])
            else:
                where_part += str(value['value'])
        elif isinstance(value, list):
            where_part += '{} in ('.format(field)
            where_value_part = ''
            where_value_args = []
            for val in value:
                if len(where_value_part) != 0:
                    where_value_part += ','
                if isinstance(val, str):
                    where_value_part += '%s'
                    where_value_args.extend([val])
                else:
                    where_value_part += str(val)
            where_part += where_value_part + ')'
            where_args.extend(where_value_args)
        else:
            where_part += '{} = '.format(field)
            if isinstance(value, str):
                where_part += '%s'
                where_args.extend([value])
            else:
                where_part += str(value)

    return where_part, where_args

def _make_set_part(data: dict) -> (str, list):
    set_part = ''
    set_args = []

    for field, value in data.items():
        if len(set_part) != 0:
            set_part += ', '
        if value is None:
            set_part += "{} = NULL".format(field)
        else:
            set_part += "{} = ".format(field)
            if isinstance(value, str):
                set_part += "%s"
                set_args.extend([value])
            else:
                set_part += str(value)

    return set_part, set_args

class ReturnType(Enum):
    NONE = 0
    ONE_ROW = 1
    ALL_ROWS = 2

class DatabaseInternal:
    def __init__(self, url: str):
        self.url = url

    def run(self, query_format: str, query_args: list, ret: ReturnType, need_commit: bool):
        result = []
        status = DatabaseError.Ok
        con = None
        try:
            con = psycopg2.connect(self.url)
            cur = con.cursor()

            logger.info('query_format = "%s"\nquery_args="%s"', query_format, ','.join(map(str, query_args)))
            cur.execute(query_format, query_args)

            if ret == ReturnType.ONE_ROW:
                result = cur.fetchone()
                logger.info('result = (%s)', ','.join(map(str, result)))
            elif ret == ReturnType.ALL_ROWS:
                result = cur.fetchall()
                logger.info('result = {%s}', ','.join('('+','.join(map(str, res))+')' for res in result))

            if need_commit:
                con.commit()

            cur.close()
        except Exception as error:
            logger.critical('Database error. Cause: %s', error)
            status = DatabaseError.InternalError
        finally:
            if con is not None:
                con.close()
            if ret == ReturnType.NONE:
                return status
            else:
                return status, result


    def select(self, get_fields, table: str, wheres: dict = {}, joins: list = []) -> (DatabaseError, list):
        fields = ', '.join(get_fields) if isinstance(get_fields, list) else _make_select_part_with_as(get_fields)

        query_format = 'SELECT {} FROM {}'.format(fields, table)
        query_args = []

        for join in joins:
            query_format += ' {} JOIN {}'.format(join['type'], join['table'])
            query_format += ' ON ' + ' and '.join('{} = {}'.format(key, value) for key, value in join['on'].items())
        
        if len(wheres) != 0:
            query_part_format, query_part_args = _make_where_part(wheres)
            query_format += ' WHERE ' + query_part_format
            query_args.extend(query_part_args)

        status, all_rows = self.run(query_format, query_args, ReturnType.ALL_ROWS, need_commit = False)

        if status != DatabaseError.Ok or len(all_rows) == 0:
            return status, []

        result = []
        column_names = get_fields if isinstance(get_fields, list) else list(get_fields.values())
        for row in all_rows:
            if len(row) == len(column_names):
                data = {}
                for i in range(len(row)):
                    data[column_names[i]] = None if row[i] is None else row[i]
                result.append(data)

        return DatabaseError.Ok, result

    def table_exists(self, name: str) -> (DatabaseError, bool):
        status, row = self.run("SELECT to_regclass('{}')".format(name), [], ReturnType.ONE_ROW, need_commit = False)

        if status != DatabaseError.Ok:
            return status, False

        if len(row) == 0:
            return DatabaseError.Ok, False
        return DatabaseError.Ok, row[0] == name

    def record_exists(self, table: str, wheres: dict) -> (DatabaseError, bool):
        status, db_record_info = self.select(['1'], table, wheres)

        if status != DatabaseError.Ok or len(db_record_info) == 0:
            return status, False

        return status, True

    def insert(self, table: str, data: dict, ret: list = []):
        columns = ', '.join(data.keys())
        query_format = 'INSERT INTO {} ({})'.format(table, columns)
        query_args = []

        query_part_format = ''
        query_part_args = []
        for value in data.values():
            if len(query_part_format) != 0:
                query_part_format += ', '
            if isinstance(value, str):
                query_part_format += "%s"
                query_part_args.extend([value])
            else:
                query_part_format += str(value)

        query_format += ' VALUES (' + query_part_format + ') '
        query_args.extend(query_part_args)

        if len(ret) != 0:
            query_format += ' RETURNING ' + ', '.join(ret)

        if len(ret) == 0:
            return self.run(query_format, query_args, ReturnType.NONE, need_commit = True)
        else:
            status, row = self.run(query_format, query_args, ReturnType.ONE_ROW, need_commit = True)

            if status != DatabaseError.Ok or len(row) == 0:
                return status, []

            returning = {}
            for i in range(len(ret)):
                returning[ret[i]] = None if row[i] is None else row[i]
            return status, returning

    def update(self, table: str, data: dict, wheres: dict) -> DatabaseError:
        query_format = 'UPDATE {}'.format(table)
        query_args = []

        query_part_format, query_part_args = _make_set_part(data)
        query_format += ' SET ' + query_part_format
        query_args.extend(query_part_args)

        query_part_format, query_part_args = _make_where_part(wheres)
        query_format += ' WHERE (' + query_part_format + ')'
        query_args.extend(query_part_args)

        return self.run(query_format, query_args, ReturnType.NONE, need_commit = True)
