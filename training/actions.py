#!/usr/bin/env python3

from database.error import DatabaseError
from training.db_manager import DatabaseManager
from utils.keyboard import KeyboardManager
import utils.utils as utils

import datetime
import logging

from telegram import Update, CallbackQuery
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext

logger = logging.getLogger(__name__)
dbm = DatabaseManager()

class TrainingActions:
    @staticmethod
    def init(config_path: str, url: str) -> DatabaseError:
        return dbm.init(config_path, url)

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

            if action == 'sell':
                sell_actions(update, req)
            elif action == 'buy':
                buy_actions(update, context, req)
            elif action == 'cancel':
                cancel_actions(update, context, req)
            elif action == 'restart':
                restart(update, context)
            elif action == 'status':
                status(update)
            elif action == 'about':
                about(update)
            elif action == 'confirm':
                confirm(update, context, req)
            elif action == 'reject':
                reject(update, context, req)
            else:
                logger.warning('Unknown command: %s', req)

        @staticmethod
        def start(update: Update, context: CallbackContext) -> None:
            if utils.is_group_chat(update):
                return
            user_data = update.message.chat
            status = dbm.add_user_info(user_data.id, user_data.username, user_data.full_name)
            if status != DatabaseError.Ok:
                _send_text(update, req = req, text = 'Возникла непредвиденная ошибка')
                return
            common_start(update, is_start = True)

#----------

def _send_text(update: Update, text: str, req: str = '') -> None:
    km = KeyboardManager(update, text)
    km.update()

#----- sell actions

def choose_place(update: Update, req: str) -> None:
    km = KeyboardManager(update, text = 'Выберите место')

    status, plases = dbm.get_all_places_info()
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о месте')
        return

    for place in plases:
        new_cmd = ','.join([req, str(place['id'])])
        km.add_button(place['name'], new_cmd)

    km.update()

def choose_session(update: Update, place_id: str, req: str) -> None:
    if not place_id.isdigit():
        logger.warning('Отправлена некорректная команда: %s', req)
        _send_text(update, req = req, text = 'Отправлена некорректная команда')
        return

    status, schedules = dbm.get_schedules(int(place_id))
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о расписании')
        return

    km = KeyboardManager(update, text = 'Выберите время')

    for schedule in schedules:
        show_name = ' '.join([schedule['info_prefix'], schedule['weekday'], schedule['time']])
        new_cmd = ','.join([req, str(schedule['id'])])
        km.add_button(show_name, new_cmd)

    km.set_back_action(req[0:req.rfind(',')])
    km.update()

def choose_date(update: Update, session_id: str, req: str) -> None:
    if not session_id.isdigit():
        logger.warning('Отправлена некорректная команда: %s', req)
        _send_text(update, req = req, text = 'Отправлена некорректная команда')
        return

    status, session_info = dbm.get_session_info(int(session_id))
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о времени')
        return

    km = KeyboardManager(update, text = 'Выберите дату')
    today = datetime.date.today()
    needed_date = utils.nearest_weekday(today, utils.weekday_id(session_info['weekday']))

    for count in range(4):
        formated_date = needed_date.strftime("%d.%m.%Y")
        km.add_button(formated_date, req + ',' + formated_date)
        needed_date += datetime.timedelta(7)

    km.set_back_action(req[0:req.rfind(',', 0, req.rfind(','))])
    km.update()

def confirm_sell(update: Update, cmd: list, req: str) -> None:
    place_id = cmd[1]; session_id = cmd[2]; date = cmd[3]
    if not place_id.isdigit() or not session_id.isdigit():
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Получены некорректные данные')
        return

    status, session_info = dbm.get_session_info(int(session_id))
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о времени')
        return

    status, place_info = dbm.get_place_info(int(place_id))
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о месте')
        return

    question = 'Вы уверены, что желаете продать слот {} {} в {}?'.format(session_info['time'], date, place_info['name'])
    km = KeyboardManager(update, text = question)
    km.add_button('Да', req + ',Y')
    km.set_back_action(req[0:req.rfind(',')])
    km.update()

def do_sell(update: Update, cmd: list, req: str) -> None:
    place_id = cmd[1]; session_id = cmd[2]; date = cmd[3]
    if not place_id.isdigit() or not session_id.isdigit():
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Получены некорректные данные')
        return

    status, session_info = dbm.get_session_info(int(session_id))
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о времени')
        return

    status, place_info = dbm.get_place_info(int(place_id))
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о месте')
        return

    status = dbm.add_sell_record(session_id = session_id, date = date, user_id = update.callback_query.message.chat.id)
    if status == DatabaseError.Ok:
        _send_text(update, req = req, text = 'Ваша заявка на продажу слота {} {} в {} успешно создана'.format(session_info['time'], date, place_info['name']))
        return
    elif status == DatabaseError.RecordExists:
        _send_text(update, req = req, text = 'Ваша заявка на продажу слота {} {} в {} уже существует'.format(session_info['time'], date, place_info['name']))
        return
    else:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Создать заявку не удалось')
        return

#----- buy actions

def choose_seller(update: Update, req: str) -> None:
    status, supplies = dbm.get_opened_supplies(date_start = datetime.date.today().strftime("%d.%m.%Y")) # !!!

    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Получить список предложений не удалось')
        return
    if len(supplies) == 0:
        _send_text(update, req = req, text = 'Не найдено ни одного предложения')
        return

    km = KeyboardManager(update, text = '', width = 1)

    text = 'Выберите предложение:'
    for place_data in supplies:
        place_name = place_data['place_name']
        text += '\n\n{}:'.format(place_name)
        for session_data in place_data['sessions']:
            info_prefix = session_data['info_prefix']
            weekday = session_data['weekday']
            time = session_data['time']
            text += '\n   {}:'.format(' '.join([info_prefix, weekday, time]))
            for date_data in session_data['dates']:
                date = date_data['date']
                text += '\n      {}:'.format(date)
                for supply in date_data['supplies']:
                    nick = supply['seller']['nick']
                    fullname = supply['seller']['fullname']
                    text += '\n         Продавец: @{} ({})'.format(nick, fullname)
                    km.add_button(' '.join([place_name, time, date, nick]), req + ',' + str(supply['id']))

    km.set_text(text)
    km.update()

def confirm_buy(update: Update, cmd: list, req: str) -> None:
    supply_id = cmd[1]
    if not supply_id.isdigit():
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Получены некорректные данные')
        return

    status, supply_info = dbm.get_supply_info(int(supply_id))
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о предложении')
        return

    question = 'Вы уверены, что желаете зафиксировать покупку слота {} {} в {} у @{} ({})?'.format(supply_info['time'], supply_info['date'], supply_info['place_name'], supply_info['seller_nick'], supply_info['seller_fullname'])
    km = KeyboardManager(update, text = question)
    km.add_button('Да', req + ',Y')
    km.set_back_action(req[0:req.rfind(',')])
    km.update()

def send_buy_confirm(update: Update, context: CallbackContext, cmd: list, req: str) -> None:
    supply_id = cmd[1]
    if not supply_id.isdigit():
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Получены некорректные данные')
        return

    status, supply_info = dbm.get_supply_info(int(supply_id)) 
    if status != DatabaseError.Ok:
        _send_text(update, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о предложении')
        return status

    # !!! Добавить проверку актуальности заявки

    user_data = update.callback_query.message.chat
    text = "Пользователь @{} ({}) желает зафиксировать за собой ваш слот на {} {} в {}. Согласуйте с ним дальнейшие действия.\n\nУбедитесь в получении оплаты перед разрешением фиксации слота.".format(user_data.username, user_data.full_name, supply_info['time'], supply_info['date'], supply_info['place_name'])

    reply_markup = KeyboardManager.make_yes_no_dialog(
        yes = {
            'text': 'Разрешить',
            'callback': 'confirm,' + supply_id + ',' + str(user_data.id)
        },
        no = {
            'text': 'Отклонить',
            'callback': 'reject,' + supply_id + ',' + str(user_data.id)
        }
    )
    context.bot.send_message(supply_info['seller_id'], text, reply_markup = reply_markup)

    _send_text(update, text = 'Пользователю @{} ({}) отправлен запрос на фиксацию слота на {} {} в {}. Согласуйте с ним дальнейшие действия.'.format(supply_info['seller_nick'], supply_info['seller_fullname'], supply_info['time'], supply_info['date'], supply_info['place_name']))

def do_buy(update: Update, supply_id: int, buyer_id: int) -> None:
    status, supply_info = dbm.get_supply_info(int(supply_id))
    if status != DatabaseError.Ok:
        _send_text(update, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о предложении')
        return status

    status = dbm.add_buy_record(supply_info['session_id'], supply_info['date'], seller_id = supply_info['seller_id'], user_id = int(buyer_id))
    if status == DatabaseError.Ok:
        _send_text(update, text = 'Фиксация слота {} {} в {} успешно произведена'.format(supply_info['time'], supply_info['date'], supply_info['place_name']))
        return status
    elif status == DatabaseError.RecordExists:
        _send_text(update, text = 'Пользователь уже воспользовался другим предложением. Попытка фиксации вашего слота отменена')
        return status
    elif status == DatabaseError.RecordUsed:
        buyer_show_name = 'Кто-то'
        if 'buyer_nick' in supply_info and 'buyer_fullname' in supply_info:
            buyer_show_name = '@{} ({})'.format(supply_info['buyer_nick'], supply_info['buyer_fullname'])
        _send_text(update, text = buyer_show_name + ' уже зафиксирован за вашим слотом. Текущее предложение отменено')
        return status
    else:
        _send_text(update, text = 'Возникла непредвиденная ошибка. Зафиксировать слот не удалось')
        return status

#----- cancel actions

def choose_cancel(update: Update, req: str) -> None:
    status, supplies = dbm.get_own_supplies(date_start = datetime.date.today().strftime("%d.%m.%Y"), user_id = update.callback_query.message.chat.id)

    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Получить список предложений не удалось')
        return
    if len(supplies) == 0:
        _send_text(update, req = req, text = 'Не найдено ни одного предложения')
        return

    text = 'Что желаете отменить?'
    km = KeyboardManager(update, text = '', width = 1)

    for place_data in supplies:
        place_name = place_data['place_name']
        text += '\n\n{}:'.format(place_name)
        for session_data in place_data['sessions']:
            info_prefix = session_data['info_prefix']
            weekday = session_data['weekday']
            time = session_data['time']
            text += '\n   {}:'.format(' '.join([info_prefix, weekday, time]))
            for date_data in session_data['dates']:
                date = date_data['date']
                text += '\n      {}:'.format(date)
                for supply in date_data['supplies']:
                    seller_nick = supply['seller']['nick']
                    seller_fullname = supply['seller']['fullname']
                    text += '\n         Продавец: @{} ({})'.format(seller_nick, seller_fullname)
                    if 'buyer' in supply:
                        buyer_nick = supply['buyer']['nick']
                        buyer_fullname = supply['buyer']['fullname']
                        text += '  Покупатель: @{} ({})'.format(buyer_nick, buyer_fullname)
                    name_action, type_action = ('Покупка', 'b') if 'buyer' in supply else ('Продажа', 's')
                    km.add_button(' '.join([name_action, place_name, time, date]), req + ',' + type_action + str(supply['id']))

    km.set_text(text)
    km.update()

def confirm_cancel(update: Update, cmd: list, req: str) -> None:
    action_id = cmd[1]
    if len(action_id) == 0 or action_id[0] not in ['b', 's'] or not action_id[1:].isdigit():
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Получены некорректные данные')
        return

    cancel_type = action_id[0]
    supply_id = action_id[1:]

    status, supply_info = dbm.get_supply_info(int(supply_id))
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о предложении')
        return

    question = 'Вы уверены, что желаете отменить '
    if cancel_type == 's':
        question += 'продажу'
    elif cancel_type == 'b':
        question += 'покупку'
    question += ' слота {} {} в {}?'.format(supply_info['time'], supply_info['date'], supply_info['place_name'])

    km = KeyboardManager(update, text = question)
    km.add_button('Да', req + ',Y')
    km.set_back_action(req[0:req.rfind(',')])
    km.update()

def do_cancel(update: Update, context: CallbackContext, cmd: list, req: str) -> None:
    action_id = cmd[1]
    if len(action_id) == 0 or action_id[0] not in ['b', 's'] or not action_id[1:].isdigit():
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Получены некорректные данные')
        return

    cancel_type = action_id[0]
    supply_id = action_id[1:]

    status, supply_info = dbm.get_supply_info(int(supply_id))
    if status != DatabaseError.Ok:
        _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о предложении')
        return

    if cancel_type == 's':
        status = dbm.cancel_sell_record(int(supply_id))
        if status == DatabaseError.Ok:
            _send_text(update, req = req, text = 'Отмена заявки на продажу слота {} {} в {} прошла успешно'.format(supply_info['time'], supply_info['date'], supply_info['place_name']))
            return
        if status == DatabaseError.RecordUsed:
            _send_text(update, req = req, text = 'У заявки на продажу слота {} {} в {} уже нашёлся покупатель. Отменить заявку невозможно'.format(supply_info['time'], supply_info['date'], supply_info['place_name']))
            return
        else:
            _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Произвести отмену не удалось')
            return
    elif cancel_type == 'b':
        status = dbm.cancel_buy_record(int(supply_id))
        if status == DatabaseError.Ok:
            status, admin_info = dbm.get_user_info_by_nick(supply_info['admin'])
            if status != DatabaseError.Ok:
                _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о тренере')
                return

            text_to_buyer = 'Отмена фиксации слота {} {} в {} прошла успешно'.format(supply_info['time'], supply_info['date'], supply_info['place_name'])
            if len(admin_info) != 0:
                text_to_seller += '. Сообщение об отмене отправлено тренеру @{} ({})'.format(admin_info['nick'], admin_info['fullname'])
            _send_text(update, req = req, text = text_to_buyer)

            user_data = update.callback_query.message.chat

            text_to_seller = 'Фиксация вашего слота {} {} в {} пользователем @{} ({}) была отменена'.format(supply_info['time'], supply_info['date'], supply_info['place_name'], user_data.username, user_data.full_name)
            if len(admin_info) != 0:
                text_to_seller += '. Сообщение об отмене отправлено тренеру @{} ({})'.format(admin_info['nick'], admin_info['fullname'])
            context.bot.send_message(supply_info['seller_id'], text_to_seller)

            if len(admin_info) != 0:
                context.bot.send_message(admin_info['id'], 'Пользователь @{} ({}) отменил фиксацию слота на {} {} в {}'.format(user_data.username, user_data.full_name, supply_info['time'], supply_info['date'], supply_info['place_name']))

            return
        else:
            _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Произвести отмену не удалось')
            return

#----- status

def _get_status() -> str:
    status, supplies = dbm.get_status(date_start = datetime.date.today().strftime("%d.%m.%Y"))
    if status != DatabaseError.Ok:
        return 'Возникла непредвиденная ошибка. Получить статус не удалось'

    if len(supplies) == 0:
        return 'Заявок на бирже нет'

    text = 'Статус:'
    for place_data in supplies:
        place_name = place_data['place_name']
        text += '\n\n{}:'.format(place_name)
        for session_data in place_data['sessions']:
            info_prefix = session_data['info_prefix']
            weekday = session_data['weekday']
            time = session_data['time']
            text += '\n   {}:'.format(' '.join([info_prefix, weekday, time]))
            for date_data in session_data['dates']:
                date = date_data['date']
                text += '\n      {}:'.format(date)
                for supply in date_data['supplies']:
                    seller_nick = supply['seller']['nick']
                    seller_fullname = supply['seller']['fullname']
                    text += '\n         Продавец: @{} ({})'.format(seller_nick, seller_fullname)
                    if 'buyer' in supply:
                        buyer_nick = supply['buyer']['nick']
                        buyer_fullname = supply['buyer']['fullname']
                        text += '  Покупатель: @{} ({})'.format(buyer_nick, buyer_fullname)

    return text

def status(update: Update) -> None:
    km = KeyboardManager(update, text = _get_status())
    km.update()

#----- about

def about(update: Update) -> None:
    _send_text(update, text = 'Работаю на сервере Heroku\nИсходный код: https://github.com/DuwazSandbox/trade-in-telegram-bot')

#----- start

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

#----- actions

def buy_actions(update: Update, context: CallbackContext, req: str) -> None:
    cmd = req.split(',')
    if len(cmd) == 1:
        choose_seller(update, req)
    elif len(cmd) == 2:
        confirm_buy(update, cmd, req)
    elif len(cmd) == 3:
        send_buy_confirm(update, context, cmd, req)
    else:
        logger.warning('Unknown command: %s', req)

def cancel_actions(update: Update, context: CallbackContext, req: str) -> None:
    cmd = req.split(',')
    if len(cmd) == 1:
        choose_cancel(update, req)
    elif len(cmd) == 2:
        confirm_cancel(update, cmd, req)
    elif len(cmd) == 3:
        do_cancel(update, context, cmd, req)
    else:
        logger.warning('Unknown command: %s', req)

def sell_actions(update: Update, req: str) -> None:
    cmd = req.split(',')
    if len(cmd) == 1:
        choose_place(update, req)
    elif len(cmd) == 2:
        choose_session(update, place_id = cmd[1], req = req)
    elif len(cmd) == 3:
        choose_date(update, session_id = cmd[2], req = req)
    elif len(cmd) == 4:
        confirm_sell(update, cmd, req)
    elif len(cmd) == 5:
        do_sell(update, cmd, req)
    else:
        logger.warning('Unknown command: %s', req)

#----- confirm & reject

def confirm(update: Update, context: CallbackContext, req: str) -> None:
    cmd = req.split(',')

    if len(cmd) != 3:
        logger.warning('Unknown command: %s', req)
        return

    supply_id = cmd[1]
    buyer_id = cmd[2]
    if not supply_id.isdigit() or not buyer_id.isdigit():
        _send_text(update, text = 'Возникла непредвиденная ошибка. Получены некорректные данные')
        return

    status = do_buy(update, int(supply_id), int(buyer_id))
    if status == DatabaseError.Ok:
        status, supply_info = dbm.get_supply_info(int(supply_id))
        if status != DatabaseError.Ok:
            _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о предложении')
            return

        status, buyer_info = dbm.get_user_info(int(buyer_id))
        if status != DatabaseError.Ok:
            _send_text(update, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о покупателе')
            return

        status, admin_info = dbm.get_user_info_by_nick(supply_info['admin'])
        if status != DatabaseError.Ok:
            _send_text(update, req = req, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о тренере')
            return

        user_data = update.callback_query.message.chat

        text_to_buyer = 'Фиксация слота {} {} в {} у @{} ({}) успешно произведена'.format(supply_info['time'], supply_info['date'], supply_info['place_name'], user_data.username, user_data.full_name)
        if len(admin_info) != 0:
            text_to_buyer += '. Сообщение о фиксации слота отправлено тренеру @{} ({})'.format(admin_info['nick'], admin_info['fullname'])
        context.bot.send_message(int(buyer_id), text_to_buyer)

        if len(admin_info) != 0:
            context.bot.send_message(admin_info['id'], '{} {} в {} вместо @{} ({}) придёт @{} ({})'.format(supply_info['time'], supply_info['date'], supply_info['place_name'], user_data.username, user_data.full_name, buyer_info['nick'], buyer_info['fullname']))
    else:
        do_reject(update, context, int(supply_id), int(buyer_id))

def do_reject(update: Update, context: CallbackContext, supply_id: int, buyer_id: int) -> None:
    status, supply_info = dbm.get_supply_info(supply_id)
    if status != DatabaseError.Ok:
        _send_text(update, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о предложении')
        return

    status, buyer_info = dbm.get_user_info(buyer_id)
    if status != DatabaseError.Ok:
        _send_text(update, text = 'Возникла непредвиденная ошибка. Не удалось получить данные о покупателе')
        return

    _send_text(update, text = 'Предложение фиксации слота {} {} в {} для @{} ({}) было отменено'.format(supply_info['time'], supply_info['date'], supply_info['place_name'], buyer_info['nick'], buyer_info['fullname']))

    user_data = update.callback_query.message.chat
    context.bot.send_message(buyer_id, 'Фиксация слота {} {} в {} у @{} ({}) была отменена'.format(supply_info['time'], supply_info['date'], supply_info['place_name'], user_data.username, user_data.full_name))

def reject(update: Update, context: CallbackContext, req: str) -> None:
    cmd = req.split(',')

    if len(cmd) != 3:
        logger.warning('Unknown command: %s', req)
        return

    supply_id = cmd[1]
    buyer_id = cmd[2]
    if not supply_id.isdigit() or not buyer_id.isdigit():
        _send_text(update, text = 'Возникла непредвиденная ошибка. Получены некорректные данные')
        return

    do_reject(update, context, int(supply_id), int(buyer_id))
