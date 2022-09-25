#!/usr/bin/env python3

from config import Config
from database_api import DatabaseAPI
from database_error import DatabaseError
import utils

class DatabaseManager():
    def init(self, config_path: str, url: str) -> DatabaseError:
        self._db = DatabaseAPI(url)
        self._config = Config(config_path) # !!! status
        status = self._db.update_data(self._config.get_pools())
        # заготовка для самообновления
        # self._config.changes_handler(lambda pools: self._db.update_data(pools))
        # self._config.watchdog_start()
        return status

    def add_sell_record(self, pool: str, weekday: str, time: str, date: str, user_id: str) -> DatabaseError:
        status, session_id = self._db.get_session_id(pool, weekday, time)
        if status != DatabaseError.Ok:
            return status
        elif session_id is None:
            return DatabaseError.InvalidData

        status, exists = self._db.sell_record_exists(date, session_id, user_id)
        if status != DatabaseError.Ok:
            return status
        elif exists:
            return DatabaseError.RecordExists

        return self._db.add_sell_record(date, session_id, user_id)

    def get_seller_ids(self, pool: str, weekday: str, time: str, date: str) -> (DatabaseError, list):
        status, session_id = self._db.get_session_id(pool, weekday, time)
        if status != DatabaseError.Ok:
            return status, []
        elif session_id is None:
            return DatabaseError.InvalidData, []

        return self._db.get_seller_ids(session_id, date)

    def get_user_info(self, user_id: str) -> (DatabaseError, dict):
        return self._db.get_user_info(user_id)

    def add_user_info(self, user_id: str, nick: str, fullname: str) -> DatabaseError:
        status, exists = self._db.user_record_exists(user_id)
        if status != DatabaseError.Ok:
            return status
        elif exists:
            return DatabaseError.Ok

        return self._db.add_user_info(user_id, nick, fullname)

    # !!! добавить оповещение продавца и тренера о покупке слота
    def add_buy_record(self, pool: str, weekday: str, time: str, date: str, seller_id: str, user_id: str) -> DatabaseError:
        status, session_id = self._db.get_session_id(pool, weekday, time)
        if status != DatabaseError.Ok:
            return status
        elif session_id is None:
            return DatabaseError.InvalidData

        status, exists = self._db.buy_record_exists(date, session_id, user_id)
        if status != DatabaseError.Ok:
            return status
        elif exists:
            return DatabaseError.RecordExists

        return self._db.add_buy_record(session_id, date, seller_id, user_id)

    def cancel_sell_record(self, pool: str, weekday: str, time: str, date: str, user_id: str) -> DatabaseError:
        status, session_id = self._db.get_session_id(pool, weekday, time)
        if status != DatabaseError.Ok:
            return status
        elif session_id is None:
            return DatabaseError.InvalidData

        return self._db.cancel_sell_record(session_id, date, user_id)

    # !!! отправить оповещение продавцу и тренеру об отмене покупки слота
    def cancel_buy_record(self, pool: str, weekday: str, time: str, date: str, user_id: str) -> DatabaseError:
        status, session_id = self._db.get_session_id(pool, weekday, time)
        if status != DatabaseError.Ok:
            return status
        elif session_id is None:
            return DatabaseError.InvalidData

        return self._db.cancel_buy_record(session_id, date, user_id)

    def get_pools_info(self) -> (DatabaseError, list):
        return self._db.get_pools_info()

    def get_schedules(self, pool: str) -> (DatabaseError, list):
        return self._db.get_schedules(pool)

    def get_status(self, date_start: str) -> (DatabaseError, dict):
        status, sellers_buyers = self._db.get_closed_deals(date_start)
        if status != DatabaseError.Ok:
            return status, {}

        status, opened_deals = self._db.get_opened_deals(date_start)
        if status != DatabaseError.Ok:
            return status, {}

        sellers_buyers += opened_deals

        if len(sellers_buyers) == 0:
            return DatabaseError.Ok, {}

        user_ids = set()
        for seller_buyer in sellers_buyers:
            user_ids.add(seller_buyer['seller'])
            if 'buyer' in seller_buyer:
                user_ids.add(seller_buyer['buyer'])

        status, users_data = self._db.get_users_info(list(user_ids))
        dicted_users_data = dict()
        for user_data in users_data:
            dicted_users_data[user_data['id']] = user_data

        records = []
        for seller_buyer in sellers_buyers:
            status, session_info = self._db.get_session_info(seller_buyer['session_id'])
            if status != DatabaseError.Ok:
                return status, {}

            status, pool_info = self._db.get_pool_info(session_info['pool_id'])
            if status != DatabaseError.Ok:
                return status, {}

            records.append(
                {
                    'seller': dicted_users_data[seller_buyer['seller']],
                    'buyer': None if 'buyer' not in seller_buyer else dicted_users_data[seller_buyer['buyer']]
                } | session_info | pool_info | {'date': seller_buyer['trade_in_date']}
            )

        # !!! rewrite
        status_data = {}
        for record in records:
            pool = record['name_ru']
            if pool not in status_data:
                status_data[pool] = {}
            group = record['group_type'] + ' ' + utils.weekday_name(record['weekday']) + ' ' + record['time']
            if group not in status_data[pool]:
                status_data[pool][group] = {}
            date = record['date']
            if date not in status_data[pool][group]:
                status_data[pool][group][date] = {}
            status_data[pool][group][date]['seller'] = record['seller']
            status_data[pool][group][date]['buyer'] = record['buyer']

        return DatabaseError.Ok, status_data