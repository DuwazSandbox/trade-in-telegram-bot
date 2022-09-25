#!/usr/bin/env python3

from telegram import Update
import datetime

def nearest_weekday(day_start: datetime.date, weekday: int) -> datetime.date:
    days_ahead = weekday - day_start.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return day_start + datetime.timedelta(days_ahead)

def weekday_name(weekday_id: str) -> str:
    name = ['ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС']
    id = int(weekday_id)
    if id < 0 or id >= 7:
        return ''
    else:
        return name[id]

def is_group_chat(update: Update) -> bool:
    if (update is not None and
        update.message is not None and
        update.message.chat is not None and
        update.message.chat.type is not None and
        update.message.chat.type == 'group'):
        return True
    return False