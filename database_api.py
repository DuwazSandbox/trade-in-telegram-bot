#!/usr/bin/env python3

from database_error import DatabaseError
from database_internal import DatabaseInternal

def _quote(text: str) -> str:
    return "'{}'".format(text)

def _reverse_date(date: str) -> str:
    return '.'.join(date.split('.')[::-1])

# db works with reverse copy
def _prepare_date(date: str) -> str:
    return _quote(_reverse_date(date))


class DatabaseAPI(DatabaseInternal):
    def __init__(self, url: str):
        super().__init__(url)
        self.cache_session = {}

    #----- Config

    # !!! Пока что только логика init
    def update_data(self, data: list) -> DatabaseError:
        status, pools = self.get_pools_info()
        if status != DatabaseError.Ok:
            return status

        for updated_data in data:
            if 'name_ru' not in updated_data:
                continue # !!! to log

            found = False
            pool_id = ''
            for pool in pools:
                if updated_data['name_ru'] == pool['name_ru']:
                    found = True
                    pool_id = pool['id']
                    break
            if not found:
                status, ret = self.insert_with_ret(table = 'pools', data = {'name_ru': _quote(updated_data['name_ru'])}, ret = ['id'])
                if status != DatabaseError.Ok:
                    return status
                if len(ret) == 0 or not 'id' in ret:
                    return DatabaseError.InternalError
                pool_id = ret['id']

            if 'schedule' not in updated_data:
                continue # !!! to log

            for schedule in updated_data['schedule']:
                status, exists = self.record_exists(table = 'sessions', wheres = {'pool_id': pool_id, 'weekday': _quote(schedule['weekday']), 'time': _quote(schedule['time'])})
                if status != DatabaseError.Ok:
                    return status
                if not exists:
                    status = self.insert(table = 'sessions', data = {'pool_id': pool_id, 'weekday': _quote(schedule['weekday']), 'time': _quote(schedule['time']), 'group_type': _quote(schedule['group_type'])})
                    if status != DatabaseError.Ok:
                        return status
        return status

    #----- pools

    def get_pools_info(self) -> (DatabaseError, list):
        return self.select(get_fields = ['id', 'name_ru'], table = 'pools')

    def get_pool_info(self, pool_id: str) -> (DatabaseError, dict):
        status, pool_info = self.select(get_fields = ['name_ru'], table = 'pools', wheres = {'id': pool_id})

        if status != DatabaseError.Ok or len(pool_info) == 0:
            return status, {}

        return status, pool_info[0]

    #----- sessions

    def get_schedules(self, pool_id: str) -> (DatabaseError, list):
        return self.select(get_fields = ['weekday', 'time', 'group_type'], table = 'sessions', wheres = {'pool_id': pool_id})

    def get_session_id(self, pool_id: str, weekday: str, time: str) -> (DatabaseError, str):
        cache_key = '#'.join([pool_id, weekday, time])
        if cache_key in self.cache_session:
            return DatabaseError.Ok, self.cache_session[cache_key]

        status, sessions = self.select(get_fields = ['id'], table = 'sessions', wheres = {'pool_id': pool_id, 'weekday': _quote(weekday), 'time': _quote(time)})

        if status != DatabaseError.Ok or len(sessions) == 0:
            return status, None

        if not 'id' in sessions[0]:
            return DatabaseError.InternalError, None

        session_id = sessions[0]['id']
        self.cache_session[cache_key] = session_id
        return status, session_id

    def get_session_info(self, session_id: str) -> (DatabaseError, dict):
        cache_key = session_id
        if cache_key in self.cache_session:
            return DatabaseError.Ok, self.cache_session[cache_key]

        status, db_session_info = self.select(get_fields = ['pool_id', 'weekday', 'time', 'group_type'], table = 'sessions', wheres = {'id': session_id})

        if status != DatabaseError.Ok or len(db_session_info) == 0:
            return status, {}

        pool_id = db_session_info[0]['pool_id']; weekday = db_session_info[0]['weekday']
        time = db_session_info[0]['time']; group_type = db_session_info[0]['group_type']
        session_info = {'pool_id': pool_id, 'weekday': weekday, 'time': time, 'group_type': group_type}
        self.cache_session[cache_key] = session_info
        return status, session_info

    #----- sell_records

    def get_seller_ids(self, session_id: str, date: str) -> (DatabaseError, list):
        status, db_seller_ids = self.select(get_fields = ['user_id'], table = 'sell_records', wheres = {'session_id': session_id, 'trade_in_date': _prepare_date(date), 'canceled': False, 'buy_id': None})

        if status != DatabaseError.Ok:
            return status, []

        seller_ids = []
        for seller_data in db_seller_ids:
            if not 'user_id' in seller_data:
                return DatabaseError.InternalError, []
            seller_ids.append(seller_data['user_id'])

        return status, seller_ids

    def sell_record_exists(self, date, session_id, user_id) -> (DatabaseError, bool):
        return self.record_exists(table = 'sell_records', wheres = {'trade_in_date': _prepare_date(date), 'session_id': session_id, 'user_id': user_id, 'canceled': False})

    def add_sell_record(self, date, session_id, user_id) -> DatabaseError:
        return self.insert(table = 'sell_records', data = {'trade_in_date': _prepare_date(date), 'user_id': user_id, 'session_id': session_id})

    def cancel_sell_record(self, session_id, date, user_id) -> DatabaseError:
        status, db_record_info = self.select(get_fields = ['id', 'buy_id'], table = 'sell_records', wheres = {'trade_in_date': _prepare_date(date), 'session_id': session_id, 'user_id': user_id,'canceled': False})

        if status != DatabaseError.Ok:
            return status

        if len(db_record_info) == 0:
            return DatabaseError.InvalidData

        if (
            not 'id' in db_record_info[0] or
            not 'buy_id' in db_record_info[0] or
            db_record_info[0]['id'] is None
           ):
            return DatabaseError.InternalError

        if db_record_info[0]['buy_id'] is not None:
            print(db_record_info[0]['buy_id'])
            return DatabaseError.RecordUsed

        return self.update(table = 'sell_records', data = {'canceled': True, 'cancel_time': 'NOW()'}, wheres = {'id': db_record_info[0]['id']})

    def get_opened_deals(self, date_start: str) -> (DatabaseError, list):
        status, opened_deals = self.select(
            get_fields = {'user_id': 'seller', 'trade_in_date': 'trade_in_date', 'session_id': 'session_id'},
            table = 'sell_records',
            wheres = {'trade_in_date': {'sign': '>', 'value': _prepare_date(date_start)}, 'canceled': False, 'buy_id': None}
        )

        if status != DatabaseError.Ok or len(opened_deals) == 0:
            return status, []

        for opened_deal in opened_deals:
            if 'trade_in_date' in opened_deal:
                opened_deal['trade_in_date'] = _reverse_date(opened_deal['trade_in_date'])

        return DatabaseError.Ok, opened_deals

    #----- buy_records

    def buy_record_exists(self, date, session_id, user_id) -> (DatabaseError, bool):
        status, db_sell_info = self.select(get_fields = ['buy_id'], table = 'sell_records', wheres = {'trade_in_date': _prepare_date(date), 'session_id': session_id, 'canceled': False})

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

    def add_buy_record(self, session_id, date, seller_id, user_id) -> DatabaseError:
        status, ret = self.insert_with_ret(table = 'buy_records', data = {'user_id': user_id}, ret = ['id'])

        if status != DatabaseError.Ok:
            return status

        if len(ret) == 0 or not 'id' in ret:
            return DatabaseError.InternalError

        buy_id = ret['id']
        return self.update(table = 'sell_records', data = {'buy_id': buy_id}, wheres = {'trade_in_date': _prepare_date(date), 'user_id': seller_id, 'session_id': session_id})

    def cancel_buy_record(self, session_id, date, user_id) -> DatabaseError:
        status, db_buy_info = self.select(get_fields = ['id', 'buy_id'], table = 'sell_records', wheres = {'trade_in_date': _prepare_date(date), 'session_id': session_id, 'user_id': user_id,'canceled': False})

        if status != DatabaseError.Ok:
            return status

        if len(db_buy_info) == 0:
            return DatabaseError.InvalidData

        if (
            not 'id' in db_buy_info[0] or
            not 'buy_id' in db_buy_info[0] or
            db_buy_info[0]['id'] is None
           ):
            return DatabaseError.InternalError

        if db_buy_info[0]['buy_id'] is None:
            return DatabaseError.InvalidData

        status = self.update(table = 'buy_records', data = {'canceled': True, 'cancel_time': 'NOW()'}, wheres = {'id': db_buy_info[0]['buy_id']})

        if status != DatabaseError.Ok:
            return status

        return self.update(table = 'sell_records', data = {'buy_id': None}, wheres = {'id': db_buy_info[0]['id']})

    #----- users

    def get_user_info(self, user_id: str) -> (DatabaseError, dict):
        status, db_user_info = self.select(get_fields = ['id', 'nick', 'fullname'], table = 'users', wheres = {'id': user_id})

        if status != DatabaseError.Ok or len(db_user_info) == 0:
            return status, {}

        return status, db_user_info[0]

    def get_users_info(self, user_ids: list) -> (DatabaseError, list):
        status, db_users_info = self.select(get_fields = ['id', 'nick', 'fullname'], table = 'users', wheres = {'id': user_ids})

        if status != DatabaseError.Ok or len(db_users_info) == 0:
            return status, {}

        return status, db_users_info

    def user_record_exists(self, user_id) -> (DatabaseError, bool):
        return self.record_exists(table = 'users', wheres = {'id': user_id})

    def add_user_info(self, user_id, nick, fullname) -> DatabaseError:
        return self.insert(table = 'users', data = {'id': user_id, 'nick': _quote(nick), 'fullname': _quote(fullname)})

    #----- combine request

    def get_closed_deals(self, date_start: str) -> (DatabaseError, list):
        status, closed_deals = self.select(
            get_fields = {'sell_records.user_id': 'seller', 'buy_records.user_id': 'buyer', 'sell_records.trade_in_date': 'trade_in_date', 'sell_records.session_id': 'session_id'},
            table = 'sell_records',
            joins = [{'table': 'buy_records', 'on': {'buy_records.id': 'sell_records.buy_id'}}],
            wheres = {'sell_records.trade_in_date': {'sign': '>', 'value': _prepare_date(date_start)}, 'sell_records.canceled': False}
        )

        if status != DatabaseError.Ok or len(closed_deals) == 0:
            return status, []

        for closed_deal in closed_deals:
            if 'trade_in_date' in closed_deal:
                closed_deal['trade_in_date'] = _reverse_date(closed_deal['trade_in_date'])

        return DatabaseError.Ok, closed_deals
