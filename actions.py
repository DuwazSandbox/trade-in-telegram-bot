#!/usr/bin/env python3

from database_error import DatabaseError
from database_manager import DatabaseManager
from keyboard import KeyboardManager
import utils

import datetime

from telegram import Update, CallbackQuery
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext

# !!! прибраться

db = DatabaseManager()

class Actions:
    @staticmethod
    def init(config_path: str, url: str) -> DatabaseError:
        return db.init(config_path, url)

    class GroupChat:
        @staticmethod
        def status(update: Update, context: CallbackContext) -> None:
            if not utils.is_group_chat(update):
                return
            status_text = _get_status()
            update.message.reply_text(status_text)

    class UserChat:
        @staticmethod
        def callback_button(update: Update, context: CallbackContext) -> None:
            req = update.callback_query.data
            index = req.find(',')
            action = req if index == -1 else req[0:index]

            if action in ['sell', 'buy', 'cancel']:
                trade_in_actions(update, req)
            elif action == 'restart':
                restart(update, context)
            elif action == 'status':
                status(update)
            elif action == 'about':
                about(update)
            else:
                print(f"Unknown command: '{req}'") # !!! use logger

        @staticmethod
        def start(update: Update, context: CallbackContext) -> None:
            if utils.is_group_chat(update):
                return
            common_start(update, is_start = True)


#----------

# Count of pools not many. We can cache it.
cached_pools_info = []
def _get_cached_pools_info() -> list:
    global cached_pools_info
    if len(cached_pools_info) == 0:
        status, cached_pools_info = db.get_pools_info() # !!!
    return cached_pools_info

#----------

def get_cached_pool_name_ru(pool_id: str) -> str:
    for pool in _get_cached_pools_info():
        if pool['id'] == pool_id:
            return pool['name_ru']
    return ''

def choose_swimming_pool(update: Update, req: str) -> None:
    km = KeyboardManager(update, text = 'Выберите бассейн')

    for pool in _get_cached_pools_info():
        km.add_button(pool['name_ru'], req + ',' + pool['id'])

    km.update()


def choose_time(update: Update, pool: str, req: str) -> None:
    km = KeyboardManager(update, text = 'Выберите время')

    status, schedules = db.get_schedules(pool)
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить время сеансов')
        return

    for schedule in schedules:
        show_name = ' '.join([schedule['group_type'], utils.weekday_name(schedule['weekday']), schedule['time']])
        new_cmd = ','.join([req, schedule['weekday'], schedule['time']])
        km.add_button(show_name, new_cmd)

    km.set_back_action(req[0:req.rfind(',')])
    km.update()

# !!! rewrite
def choose_date(update: Update, weekday: str, req: str) -> None:
    km = KeyboardManager(update, text = 'Выберите дату')
    today = datetime.date.today()
    needed_date = utils.nearest_weekday(today, int(weekday))

    for count in range(4):
        formated_date = needed_date.strftime("%d.%m.%Y")
        km.add_button(formated_date, req + ',' + formated_date)
        needed_date += datetime.timedelta(7)

    km.set_back_action(req[0:req.rfind(',',0,req.rfind(','))])
    km.update()

#----------

def _send_text(update: Update, text: str, req: str = '') -> None:
    km = KeyboardManager(update, text)
    km.update()

#----------

def confirm_sell(update: Update, cmd: list, req: str) -> None:
    pool = cmd[1]; time = cmd[3]; date = cmd[4]
    question = 'Вы уверены, что желаете продать слот {} {} в бассейне {}?'.format(time, date, get_cached_pool_name_ru(pool))
    km = KeyboardManager(update, text = question)
    km.add_button('Да', req + ',Y')
    km.set_back_action(req[0:req.rfind(',')])
    km.update()

def do_sell(update: Update, cmd: list, req: str) -> None:
    user_data = update.callback_query.message.chat
    status = db.add_user_info(str(user_data.id), user_data.username, user_data.full_name)
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось сохранить ваши данные')
        return

    pool = cmd[1]; weekday = cmd[2]; time = cmd[3]; date = cmd[4]
    status = db.add_sell_record(pool = pool, weekday = weekday, time = time, date = date, user_id = user_data.id)
    if status == DatabaseError.Ok:
        _send_text(update, req = req, text = 'Ваша заявка на продажу слота {} {} в бассейн {} успешно создана'.format(time, date, get_cached_pool_name_ru(pool)))
        return
    elif status == DatabaseError.RecordExists:
        _send_text(update, req = req, text = 'Ваша заявка на продажу слота {} {} в бассейн {} уже существует'.format(time, date, get_cached_pool_name_ru(pool)))
        return
    else:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Создать заявку не удалось')
        return

#----------

def choose_seller(update: Update, cmd: list, req: str) -> None:
    pool = cmd[1]; weekday = cmd[2]; time = cmd[3]; date = cmd[4]
    status, seller_ids = db.get_seller_ids(pool = pool, weekday = weekday, time = time, date = date)
    if status == DatabaseError.Ok and len(seller_ids) > 0:
        km = KeyboardManager(update, text = '')

        text = 'Выберите продавца:'
        for seller_id in seller_ids:
            status, seller_info = db.get_user_info(seller_id)
            if status != DatabaseError.Ok:
                _send_text(update, req = req, text = 'Не удалось получить информацию о продавцах слота на {} {} в бассейн {}')
                return
            else:
                text += '\n@{} ({})'.format(seller_info['nick'], seller_info['fullname'])
                km.add_button(seller_info['fullname'], req + ',' + seller_id)

        km.set_text(text)
        km.set_back_action(req[0:req.rfind(',')])
        km.update()
        return
    elif status == DatabaseError.Ok and len(seller_ids) == 0:
        _send_text(update, req = req, text = 'Не найдено ни одного продавца слота на {} {} в бассейн {}'.format(time, date, get_cached_pool_name_ru(pool)))
        return
    else:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Получить список заявок не удалось')
        return

def confirm_buy(update: Update, cmd: list, req: str) -> None:
    pool = cmd[1]; time = cmd[3]; date = cmd[4]; seller_id = cmd[5]

    pool_name_ru = get_cached_pool_name_ru(pool)

    status, seller_info = db.get_user_info(seller_id)
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о продавце')
        return

    question = 'Вы уверены, что желаете зафиксировать покупку слота {} {} в бассейн {} у @{} ({})?'.format(time, date, pool_name_ru, seller_info['nick'], seller_info['fullname'])
    km = KeyboardManager(update, text = question)
    km.add_button('Да', req + ',Y')
    km.set_back_action(req[0:req.rfind(',')])
    km.update()

def do_buy(update: Update, cmd: list, req: str) -> None:
    user_data = update.callback_query.message.chat
    status = db.add_user_info(str(user_data.id), user_data.username, user_data.full_name)
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось сохранить ваши данные')
        return

    pool = cmd[1]; weekday = cmd[2]; time = cmd[3]; date = cmd[4]; seller_id = cmd[5]
    status = db.add_buy_record(pool = pool, weekday = weekday, time = time, date = date, seller_id = seller_id, user_id = user_data.id)
    if status == DatabaseError.Ok:
        _send_text(update, req = req, text = 'Фиксация слота {} {} в бассейн {} успешно произведена'.format(time, date, get_cached_pool_name_ru(pool), seller_id))
        return
    elif status == DatabaseError.RecordExists:
        _send_text(update, req = req, text = 'У вас уже имеется фиксация слота {} {} в бассейн {}'.format(time, date, get_cached_pool_name_ru(pool)))
        return
    elif status == DatabaseError.RecordUsed:
        _send_text(update, req = req, text = 'Возможно кто-то уже зафиксировал этот слот. Зафиксировать слот за вами не удалось')
        return
    else:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Зафиксировать слот не удалось')
        return

#----------

def choose_cancel_type(update: Update, req: str) -> None:
    km = KeyboardManager(update, text = 'Что желаете отменить?')
    km.add_button('Продажу', req + ',s')
    km.add_button('Покупку', req + ',b')
    km.set_back_action(req[0:req.rfind(',')])
    km.update()

def confirm_cancel(update: Update, cmd: list, req: str) -> None:
    pool = cmd[1]; time = cmd[3]; date = cmd[4]; cancel_type = cmd[5]
    question = 'Вы уверены, что желаете отменить '
    question += 'продажу' if cancel_type == 's' else 'покупку'
    question += ' слота {} {} в бассейн {}?'.format(time, date, get_cached_pool_name_ru(pool))
    km = KeyboardManager(update, text = question)
    km.add_button('Да', req + ',Y')
    km.set_back_action(req[0:req.rfind(',')])
    km.update()

def do_cancel(update: Update, cmd: list, req: str) -> None:
    pool = cmd[1]; weekday = cmd[2]; time = cmd[3]; date = cmd[4]; cancel_type = cmd[5]
    if cancel_type == 's':
        status = db.cancel_sell_record(pool = pool, weekday = weekday, time = time, date = date, user_id = update.callback_query.message.chat.id)
        if status == DatabaseError.Ok:
            _send_text(update, req = req, text = 'Отмена заявки на продажу слота {} {} в бассейн {} прошла успешно'.format(time, date, get_cached_pool_name_ru(pool)))
            return
        if status == DatabaseError.RecordUsed:
            _send_text(update, req = req, text = 'У заявки на продажу слота {} {} в бассейн {} уже нашёлся покупатель. Отменить заявку невозможно'.format(time, date, get_cached_pool_name_ru(pool)))
            return
        else:
            _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Произвести отмену не удалось')
            return
    elif cancel_type == 'b':
        status = db.cancel_buy_record(pool = pool, weekday = weekday, time = time, date = date, user_id = update.callback_query.message.chat.id)
        if status == DatabaseError.Ok:
            _send_text(update, req = req, text = 'Отмена фиксации слота {} {} в бассейн {} прошла успешно'.format(time, date, get_cached_pool_name_ru(pool)))
            return
        else:
            _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Произвести отмену не удалось')
            return
    else:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Произвести отмену не удалось')
        return

#----------

def _get_status() -> str:
    status, status_data = db.get_status(date_start = datetime.date.today().strftime("%d.%m.%Y"))
    if status != DatabaseError.Ok:
        return 'Возникла непредвиденная ошибка. Получить статус не удалось'

    if len(status_data) == 0:
        return 'Заявок на бирже нет'

    output = 'Статус:'
    for pool, pool_data in status_data.items():
        output += '\n\n{}:'.format(pool)
        for group, group_data in pool_data.items():
            output += '\n   {}:'.format(group)
            for date, date_data in group_data.items():
                output += '\n      {}:'.format(date)
                output += '\n         Продавец: @{} ({})'.format(date_data['seller']['nick'], date_data['seller']['fullname'])
                if date_data['buyer'] is not None:
                    output += ' Покупатель: @{} ({})'.format(date_data['buyer']['nick'], date_data['buyer']['fullname'])

    return output

def status(update: Update) -> None:
    km = KeyboardManager(update, text = _get_status())
    km.update()


def about(update: Update) -> None:
    _send_text(update, text = 'Работаю на сервере Heroku\nИсходный код: ')


def common_start(update: Update, is_start: bool) -> None:
    km = KeyboardManager(update, text = 'Чего изволите?')
    km.add_button('Продать',  'sell')
    km.add_button('Купить',   'buy')
    km.add_button('Отменить', 'cancel')
    km.add_button('Статус',   'status')
    km.add_button('О боте',   'about')
    km.set_show_button_home(False)
    km.set_is_first_msg(is_start)
    km.update()

def restart(update: Update, context: CallbackContext) -> None:
    common_start(update, is_start = False)


def sell_actions(update: Update, cmd: list, req: str) -> None:
    if len(cmd) == 5:
        confirm_sell(update, cmd, req)
    elif len(cmd) == 6:
        do_sell(update, cmd, req)
    else:
        print(f"Unknown command: '{req}'") # !!! use logger

def buy_actions(update: Update, cmd: list, req: str) -> None:
    if len(cmd) == 5:
        choose_seller(update, cmd, req)
    elif len(cmd) == 6:
        confirm_buy(update, cmd, req)
    elif len(cmd) == 7:
        do_buy(update, cmd, req)
    else:
        print(f"Unknown command: '{req}'") # !!! use logger

def cancel_actions(update: Update, cmd: list, req: str) -> None:
    if len(cmd) == 5:
        choose_cancel_type(update, req)
    elif len(cmd) == 6:
        confirm_cancel(update, cmd, req)
    elif len(cmd) == 7:
        do_cancel(update, cmd, req)
    else:
        print(f"Unknown command: '{req}'") # !!! use logger

def trade_in_actions(update: Update, req: str) -> None:
    cmd = req.split(',')
    action = cmd[0]

    if len(cmd) == 1:
        choose_swimming_pool(update, req)
    elif len(cmd) == 2:
        choose_time(update, pool = cmd[1], req = req)
    elif len(cmd) == 4:
        choose_date(update, weekday = cmd[2], req = req)
    elif action == 'sell':
        sell_actions(update, cmd, req)
    elif action == 'buy':
        buy_actions(update, cmd, req)
    elif action == 'cancel':
        cancel_actions(update, cmd, req)
    else:
        print(f"Unknown command: '{req}'") # !!! use logger
