#!/usr/bin/env python3

from database.error import DatabaseError
from database.internal import DatabaseInternal, ReturnType

import logging

logger = logging.getLogger(__name__)

# db works with reverse copy
def _reverse_date(date: str) -> str:
    return '.'.join(date.split('.')[::-1])


# id equals telegram user id
QUERY_CREATE_TABLE_USERS = (
    'CREATE TABLE users ('
        'id BIGINT PRIMARY KEY, '
        'nick VARCHAR (255) UNIQUE NOT NULL, '
        'fullname VARCHAR (255) NOT NULL'
    ')'
)

QUERY_CREATE_TABLE_PLACES = (
    'CREATE TABLE places ('
        'id serial PRIMARY KEY, '
        'name VARCHAR (255) UNIQUE NOT NULL'
    ')'
)

QUERY_CREATE_TABLE_SESSIONS = (
    'CREATE TABLE sessions ('
        'id serial PRIMARY KEY, '
        'place_id INT NOT NULL REFERENCES places (id), '
        'time VARCHAR (5) NOT NULL, '
        'weekday VARCHAR (3) NOT NULL, '
        'info_prefix VARCHAR (50)'
    ')'
)

QUERY_CREATE_TABLE_BUY_RECORDS = (
    'CREATE TABLE buy_records ('
        'id serial PRIMARY KEY, '
        'record_time TIMESTAMP NOT NULL DEFAULT NOW(), '
        'user_id BIGINT NOT NULL REFERENCES users (id), '
        'canceled BOOLEAN NOT NULL DEFAULT FALSE, '
        'cancel_time TIMESTAMP'
    ')'
)

QUERY_CREATE_TABLE_SELL_RECORDS = (
    'CREATE TABLE sell_records ('
        'id serial PRIMARY KEY, '
        'record_time TIMESTAMP NOT NULL DEFAULT NOW(), '
        'user_id BIGINT NOT NULL REFERENCES users (id), '
        'session_id INT NOT NULL REFERENCES sessions (id), '
        'trade_in_date VARCHAR (10) NOT NULL, '
        'price INT, '
        'buy_id INT REFERENCES buy_records (id), '
        'canceled BOOLEAN NOT NULL DEFAULT FALSE, '
        'cancel_time TIMESTAMP'
    ')'
)

class DatabaseAPI(DatabaseInternal):
    def __init__(self, url: str):
        super().__init__(url)

    def _check_and_create_table(self, table_name: str, create_query: str) -> DatabaseError:
        status, exists = self.table_exists(name = table_name)
        if status != DatabaseError.Ok:
            return status
        if not exists:
            status, self.run(create_query, [], ReturnType.NONE, need_commit = True)
            if status != DatabaseError.Ok:
                return status
        return DatabaseError.Ok

    def init_tables(self) -> DatabaseError:
        tables = [
            {'name':'users',        'query':QUERY_CREATE_TABLE_USERS},
            {'name':'places',       'query':QUERY_CREATE_TABLE_PLACES},
            {'name':'sessions',     'query':QUERY_CREATE_TABLE_SESSIONS},
            {'name':'buy_records',  'query':QUERY_CREATE_TABLE_BUY_RECORDS},
            {'name':'sell_records', 'query':QUERY_CREATE_TABLE_SELL_RECORDS},
        ]
        for table in tables:
            status = self._check_and_create_table(table['name'], table['query'])
            if status != DatabaseError.Ok:
                return status

        return DatabaseError.Ok

    #----- Config

    # !!! Пока что только логика init
    def update_data(self, data: list) -> DatabaseError:
        status, places = self.get_all_places_info()
        if status != DatabaseError.Ok:
            return status

        for updated_data in data:
            if 'places' not in updated_data:
                logger.critical('not found any places')
                continue

            for updated_places_data in updated_data['places']:
                if 'name' not in updated_places_data:
                    logger.critical('unknown place name')
                    continue

                place_id = None
                for place in places:
                    if updated_places_data['name'] == place['name']:
                        place_id = place['id']
                        break
                if place_id is None:
                    status, ret = self.insert(
                        table = 'places',
                        data = {'name': updated_places_data['name']},
                        ret = ['id']
                    )
                    if status != DatabaseError.Ok:
                        return status
                    if len(ret) == 0 or not 'id' in ret:
                        return DatabaseError.InternalError
                    place_id = ret['id']

                if 'schedule' not in updated_places_data:
                    logger.critical('unknown schedule in place %s', updated_places_data['name'])
                    continue

                for schedule in updated_places_data['schedule']:
                    status, exists = self.record_exists(
                        table = 'sessions',
                        wheres = {
                            'place_id': place_id,
                            'weekday': schedule['weekday'],
                            'time': schedule['time']
                        }
                    )
                    if status != DatabaseError.Ok:
                        return status
                    if not exists:
                        status = self.insert(
                            table = 'sessions',
                            data = {
                                'place_id': place_id,
                                'weekday': schedule['weekday'],
                                'time': schedule['time'],
                                'info_prefix': schedule['info_prefix']
                            }
                        )
                        if status != DatabaseError.Ok:
                            return status
        return status

    #----- places

    def get_all_places_info(self) -> (DatabaseError, list):
        return self.select(
            get_fields = ['id', 'name'],
            table = 'places'
        )

    def get_place_info(self, place_id: int) -> (DatabaseError, dict):
        status, place_info = self.select(
            get_fields = ['id', 'name'],
            table = 'places',
            wheres = {'id': place_id}
        )

        if status != DatabaseError.Ok or len(place_info) == 0:
            return status, {}

        return status, place_info[0]

    def get_places_info(self, place_ids: list) -> (DatabaseError, list):
        status, places_info = self.select(
            get_fields = ['id', 'name'],
            table = 'places',
            wheres = {'id': place_ids}
        )

        if status != DatabaseError.Ok or len(places_info) == 0:
            return status, {}

        return status, places_info

    #----- sessions

    def get_schedules(self, place_id: int) -> (DatabaseError, list):
        return self.select(
            get_fields = ['id', 'weekday', 'time', 'info_prefix'],
            table = 'sessions',
            wheres = {'place_id': place_id}
        )

    def get_session_info(self, session_id: int) -> (DatabaseError, dict):
        status, sessions_info = self.select(
            get_fields = ['id', 'place_id', 'weekday', 'time', 'info_prefix'],
            table = 'sessions',
            wheres = {'id': session_id}
        )

        if status != DatabaseError.Ok or len(sessions_info) == 0:
            return status, {}

        return status, sessions_info[0]

    def get_sessions_info(self, session_ids: list) -> (DatabaseError, list):
        status, sessions_info = self.select(
            get_fields = ['id', 'place_id', 'weekday', 'time', 'info_prefix'],
            table = 'sessions',
            wheres = {'id': session_ids}
        )

        if status != DatabaseError.Ok or len(sessions_info) == 0:
            return status, {}

        return status, sessions_info

    #----- sell_records

    def get_sell_record(self, record_id: int) -> (DatabaseError, dict):
        status, sell_records = self.select(
            get_fields = ['id', 'user_id', 'session_id', 'trade_in_date'],
            table = 'sell_records',
            wheres = {'id': record_id}
        )

        if status != DatabaseError.Ok or len(sell_records) == 0:
            return status, {}

        if 'trade_in_date' in sell_records[0]:
            sell_records[0]['trade_in_date'] = _reverse_date(sell_records[0]['trade_in_date'])

        return status, sell_records[0]

    def sell_record_exists(self, date: str, session_id: int, user_id: int) -> (DatabaseError, bool):
        return self.record_exists(
            table = 'sell_records',
            wheres = {
                'trade_in_date': _reverse_date(date),
                'session_id': session_id,
                'user_id': user_id,
                'canceled': False
            }
        )

    def add_sell_record(self, date: str, session_id: int, user_id: int) -> DatabaseError:
        return self.insert(
            table = 'sell_records',
            data = {
                'trade_in_date': _reverse_date(date),
                'user_id': user_id,
                'session_id': session_id
            }
        )

    def cancel_sell_record(self, record_id: int) -> DatabaseError:
        status, record_info = self.select(
            get_fields = ['buy_id'],
            table = 'sell_records',
            wheres = {
                'id': record_id,
                'canceled': False
            }
        )

        if status != DatabaseError.Ok:
            return status

        if len(record_info) == 0:
            return DatabaseError.InvalidData

        if not 'buy_id' in record_info[0]:
            return DatabaseError.InternalError

        if record_info[0]['buy_id'] is not None:
            return DatabaseError.RecordUsed

        return self.update(
            table = 'sell_records',
            data = {
                'canceled': True,
                'cancel_time': 'NOW()'
            },
            wheres = {'id': record_id}
        )

    #----- buy_records

    def buy_record_exists(self, date: str, session_id: int, user_id: int) -> (DatabaseError, bool):
        status, db_sell_info = self.select(
            get_fields = ['buy_id'],
            table = 'sell_records',
            wheres = {
                'trade_in_date': _reverse_date(date),
                'session_id': session_id,
                'canceled': False
            }
        )

        if status != DatabaseError.Ok or len(db_sell_info) == 0:
            return status, False

        # usually 1-2 iterations
        for sell_info in db_sell_info:
            buy_id = sell_info['buy_id']
            if buy_id is None:
                continue
            status, exists = self.record_exists(table = 'buy_records', wheres = {'id': buy_id, 'user_id': user_id})
            if status != DatabaseError.Ok:
                return status, False
            if exists:
                return status, True

        return DatabaseError.Ok, False

    def add_buy_record(self, session_id: int, date: str, seller_id: int, user_id: int) -> DatabaseError:
        status, ret = self.insert(
            table = 'buy_records',
            data = {'user_id': user_id},
            ret = ['id']
        )

        if status != DatabaseError.Ok:
            return status

        if len(ret) == 0 or not 'id' in ret:
            return DatabaseError.InternalError

        buy_id = ret['id']
        return self.update(
            table = 'sell_records',
            data = {'buy_id': buy_id},
            wheres = {
                'trade_in_date': _reverse_date(date),
                'user_id': seller_id,
                'session_id': session_id
            }
        )

    def cancel_buy_record(self, record_id: int) -> DatabaseError:
        status, record_info = self.select(
            get_fields = ['buy_id'],
            table = 'sell_records',
            wheres = {
                'id': record_id,
                'canceled': False
            }
        )

        if status != DatabaseError.Ok:
            return status

        if len(record_info) == 0:
            return DatabaseError.InvalidData

        if not 'buy_id' in record_info[0]:
            return DatabaseError.InternalError

        if record_info[0]['buy_id'] is None:
            return DatabaseError.InvalidData

        status = self.update(
            table = 'buy_records',
            data = {
                'canceled': True,
                'cancel_time': 'NOW()'
            },
            wheres = {'id': record_info[0]['buy_id']}
        )

        if status != DatabaseError.Ok:
            return status

        return self.update(
            table = 'sell_records',
            data = {'buy_id': None},
            wheres = {'id': record_id}
        )

    #----- users

    def get_user_info(self, user_id: int) -> (DatabaseError, dict):
        status, db_user_info = self.select(
            get_fields = ['id', 'nick', 'fullname'],
            table = 'users',
            wheres = {'id': user_id}
        )

        if status != DatabaseError.Ok or len(db_user_info) == 0:
            return status, {}

        return status, db_user_info[0]

    def get_users_info(self, user_ids: list) -> (DatabaseError, list):
        status, db_users_info = self.select(
            get_fields = ['id', 'nick', 'fullname'],
            table = 'users',
            wheres = {'id': user_ids}
        )

        if status != DatabaseError.Ok or len(db_users_info) == 0:
            return status, {}

        return status, db_users_info

    def user_record_exists(self, user_id: int) -> (DatabaseError, bool):
        return self.record_exists(
            table = 'users',
            wheres = {'id': user_id}
        )

    def add_user_info(self, user_id: int, nick: str, fullname: str) -> DatabaseError:
        return self.insert(
            table = 'users',
            data = {
                'id': user_id,
                'nick': nick,
                'fullname': fullname
            }
        )

    #----- requests with deals

    def get_opened_deals(self, date_start: str, user_id: int = None) -> (DatabaseError, list):
        wheres = {
            'trade_in_date': {
                'sign': '>',
                'value': _reverse_date(date_start)
            },
            'canceled': False,
            'buy_id': None
        }
        if user_id is not None:
            wheres['user_id'] = user_id

        status, opened_deals = self.select(
            get_fields = {
                'id': 'id',
                'user_id': 'seller',
                'trade_in_date': 'trade_in_date',
                'session_id': 'session_id'
            },
            table = 'sell_records',
            wheres = wheres
        )

        if status != DatabaseError.Ok or len(opened_deals) == 0:
            return status, []

        for opened_deal in opened_deals:
            if 'trade_in_date' in opened_deal:
                opened_deal['trade_in_date'] = _reverse_date(opened_deal['trade_in_date'])

        return DatabaseError.Ok, opened_deals

    def get_closed_deals(self, date_start: str, user_id: int = None) -> (DatabaseError, list):
        wheres = {
            'sell_records.trade_in_date': {
                'sign': '>',
                'value': _reverse_date(date_start)
            },
            'buy_records.canceled': False
        }
        if user_id is not None:
            wheres['buy_records.user_id'] = user_id

        status, closed_deals = self.select(
            get_fields = {
                'sell_records.id': 'id',
                'buy_records.id': 'buy_id',
                'sell_records.user_id': 'seller',
                'buy_records.user_id': 'buyer',
                'sell_records.trade_in_date': 'trade_in_date',
                'sell_records.session_id': 'session_id'
            },
            table = 'sell_records',
            joins = [
                {
                    'type': 'INNER',
                    'table': 'buy_records',
                    'on': {
                        'buy_records.id': 'sell_records.buy_id'
                    }
                }
            ],
            wheres = wheres
        )

        if status != DatabaseError.Ok or len(closed_deals) == 0:
            return status, []

        for closed_deal in closed_deals:
            if 'trade_in_date' in closed_deal:
                closed_deal['trade_in_date'] = _reverse_date(closed_deal['trade_in_date'])

        return DatabaseError.Ok, closed_deals
