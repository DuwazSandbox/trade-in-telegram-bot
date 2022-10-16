#!/usr/bin/env python3

from training.db_api import DatabaseAPI
from database.error import DatabaseError
from utils.config import Config
import utils.utils

def _get_elem_with_key_value(placement: list, key, value) -> (bool, dict):
    for place in placement:
        if key in place and place[key] == value:
            return False, place
    new_place = dict()
    new_place[key] = value
    placement.append(new_place)
    return True, new_place


class DatabaseManager():
    def init(self, config_path: str, url: str) -> DatabaseError:
        self._config = Config(config_path) # !!! status
        self._db = DatabaseAPI(url)
        status = self._db.init_tables()
        status = self._db.update_data(self._config.get_data())
        # заготовка для самообновления
        # self._config.changes_handler(lambda places: self._db.update_data(places))
        # self._config.watchdog_start()
        return status

    #----- places

    def get_place_info(self, place_id: int) -> (DatabaseError, dict):
        return self._db.get_place_info(place_id)

    def get_all_places_info(self) -> (DatabaseError, list):
        return self._db.get_all_places_info()

    #----- sessions

    def get_schedules(self, place_id: int) -> (DatabaseError, list):
        return self._db.get_schedules(place_id)

    def get_session_info(self, session_id: int) -> (DatabaseError, dict):
        return self._db.get_session_info(session_id)

    #----- supplies

    def add_sell_record(self, session_id: int, date: str, user_id: int) -> DatabaseError:
        status, exists = self._db.sell_record_exists(date, session_id, user_id)
        if status != DatabaseError.Ok:
            return status
        elif exists:
            return DatabaseError.RecordExists

        return self._db.add_sell_record(date, session_id, user_id)

    def add_buy_record(self, session_id: int, date: str, seller_id: int, user_id: int) -> DatabaseError:
        status, exists = self._db.buy_record_exists(date, session_id, user_id)
        if status != DatabaseError.Ok:
            return status

        if exists:
            return DatabaseError.RecordExists

        return self._db.add_buy_record(session_id, date, seller_id, user_id)

    def cancel_sell_record(self, record_id: int) -> DatabaseError:
        return self._db.cancel_sell_record(record_id)

    # !!! отправить оповещение продавцу и тренеру об отмене фиксации слота
    def cancel_buy_record(self, record_id: int) -> DatabaseError:
        return self._db.cancel_buy_record(record_id)

    def get_supply_info(self, supply_id: int) -> (DatabaseError, dict):
        status, supply_info = self._db.get_sell_record(supply_id)
        if status != DatabaseError.Ok:
            return status, {}

        status, session_info = self.get_session_info(supply_info['session_id'])
        if status != DatabaseError.Ok:
            return status, {}

        status, place_info = self.get_place_info(session_info['place_id'])
        if status != DatabaseError.Ok:
            return status, {}

        status, user_info = self.get_user_info(supply_info['user_id'])
        if status != DatabaseError.Ok:
            return status, {}

        return DatabaseError.Ok, {
            'time': session_info['time'],
            'admin': session_info['admin'],
            'date': supply_info['trade_in_date'],
            'place_name': place_info['name'],
            'seller_id': user_info['id'],
            'seller_nick': user_info['nick'],
            'seller_fullname': user_info['fullname'],
            'session_id': supply_info['session_id']
        }

    def _get_supplies(self, date_start: str, closed_deals: bool, opened_deals: bool, user_id: int = None) -> (DatabaseError, list):
        supplies = []

        if closed_deals:
            status, db_closed_deals = self._db.get_closed_deals(date_start, user_id)
            if status != DatabaseError.Ok:
                return status, []
            supplies.extend(db_closed_deals)

        if opened_deals:
            status, db_opened_deals = self._db.get_opened_deals(date_start, user_id)
            if status != DatabaseError.Ok:
                return status, []
            supplies.extend(db_opened_deals)

        return DatabaseError.Ok, supplies

    def _get_all_supply_users_info(self, supplies: list) -> (DatabaseError, dict):
        if len(supplies) == 0:
            return DatabaseError.Ok, {}

        user_ids = set()
        for supply in supplies:
            user_ids.add(supply['seller'])
            if 'buyer' in supply:
                user_ids.add(supply['buyer'])

        status, users_data = self._db.get_users_info(list(user_ids))

        if status != DatabaseError.Ok or len(users_data) == 0:
            return status, {}

        all_supply_users_info = dict()
        for user_data in users_data:
            all_supply_users_info[user_data['id']] = user_data

        return DatabaseError.Ok, all_supply_users_info

    def _get_all_supply_sessions_info(self, supplies: list) -> (DatabaseError, dict):
        if len(supplies) == 0:
            return DatabaseError.Ok, {}

        session_ids = set()
        for supply in supplies:
            session_ids.add(supply['session_id'])

        status, sessions_data = self._db.get_sessions_info(list(session_ids))

        if status != DatabaseError.Ok or len(sessions_data) == 0:
            return status, {}

        all_supply_sessions_info = dict()
        for session_data in sessions_data:
            all_supply_sessions_info[session_data['id']] = session_data

        return DatabaseError.Ok, all_supply_sessions_info

    def _get_all_supply_places_info(self, sessions_info: dict) -> (DatabaseError, list):
        if len(sessions_info) == 0:
            return DatabaseError.Ok, []

        place_ids = set()
        for session_info in sessions_info.values():
            place_ids.add(session_info['place_id'])

        status, places_data = self._db.get_places_info(list(place_ids))

        if status != DatabaseError.Ok or len(places_data) == 0:
            return status, []

        all_supply_places_info = dict()
        for place_data in places_data:
            all_supply_places_info[place_data['id']] = place_data

        return DatabaseError.Ok, all_supply_places_info

    def _get_all_supply_additional_info(self, supplies: list) -> (DatabaseError, dict):
        if len(supplies) == 0:
            return DatabaseError.Ok, {}

        additional_info = dict()

        status, users_info = self._get_all_supply_users_info(supplies)
        if status != DatabaseError.Ok:
            return status, {}
        additional_info['users_info'] = users_info

        status, sessions_info = self._get_all_supply_sessions_info(supplies)
        if status != DatabaseError.Ok:
            return status, {}
        additional_info['sessions_info'] = sessions_info

        status, places_info = self._get_all_supply_places_info(sessions_info)
        if status != DatabaseError.Ok:
            return status, {}
        additional_info['places_info'] = places_info

        return DatabaseError.Ok, additional_info

    def _make_supplies_info(self, supplies) -> (DatabaseError, list):
        if len(supplies) == 0:
            return DatabaseError.Ok, []

        status, additional_info = self._get_all_supply_additional_info(supplies)
        if status != DatabaseError.Ok:
            return status, []

        supplies_info = list()
        for supply in supplies:
            session_id = supply['session_id']
            place_id = additional_info['sessions_info'][session_id]['place_id']
            is_new_place, place_info = _get_elem_with_key_value(supplies_info, key='place_id', value=place_id)

            if is_new_place:
                place_info['place_name'] = additional_info['places_info'][place_id]['name']
                place_info['sessions'] = list()

            is_new_session, session_info = _get_elem_with_key_value(place_info['sessions'], key='id', value=supply['session_id'])

            if is_new_session:
                session_info['info_prefix'] = additional_info['sessions_info'][session_id]['info_prefix']
                session_info['weekday'] = additional_info['sessions_info'][session_id]['weekday']
                session_info['time'] = additional_info['sessions_info'][session_id]['time']
                session_info['dates'] = list()

            is_new_date, date_info = _get_elem_with_key_value(session_info['dates'], key='date', value=supply['trade_in_date'])

            if is_new_date:
                date_info['supplies'] = list()

            supply_info = dict()
            supply_info['id'] = supply['id']
            supply_info['seller'] = additional_info['users_info'][supply['seller']]
            if 'buyer' in supply:
                supply_info['buyer'] = additional_info['users_info'][supply['buyer']]
            date_info['supplies'].append(supply_info)

        return DatabaseError.Ok, supplies_info

    def get_status(self, date_start: str) -> (DatabaseError, list):
        status, supplies = self._get_supplies(date_start = date_start, closed_deals = True, opened_deals = True)
        if status != DatabaseError.Ok or len(supplies) == 0:
            return status, []

        return self._make_supplies_info(supplies)

    def get_opened_supplies(self, date_start: str) -> (DatabaseError, list):
        status, supplies = self._get_supplies(date_start = date_start, closed_deals = False, opened_deals = True)
        if status != DatabaseError.Ok:
            return status, {}

        return self._make_supplies_info(supplies)

    def get_own_supplies(self, date_start: str, user_id: int) -> (DatabaseError, list):
        status, supplies = self._get_supplies(date_start = date_start, closed_deals = True, opened_deals = True, user_id = user_id)
        if status != DatabaseError.Ok:
            return status, {}

        return self._make_supplies_info(supplies)

    #----- users

    def get_user_info(self, user_id: int) -> (DatabaseError, dict):
        return self._db.get_user_info(user_id)

    def get_user_info_by_nick(self, user_nick: str) -> (DatabaseError, dict):
        return self._db.get_user_info_by_nick(user_nick)

    def add_user_info(self, user_id: int, nick: str, fullname: str) -> DatabaseError:
        status, exists = self._db.user_record_exists(user_id)
        if status != DatabaseError.Ok:
            return status
        elif exists:
            return DatabaseError.Ok

        return self._db.add_user_info(user_id, nick, fullname)
