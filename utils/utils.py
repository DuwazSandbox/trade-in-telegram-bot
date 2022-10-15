#!/usr/bin/env python3

from telegram import Chat, Update
import datetime

def nearest_weekday(day_start: datetime.date, weekday: int) -> datetime.date:
    days_ahead = weekday - day_start.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return day_start + datetime.timedelta(days_ahead)

def weekday_id(weekday_name: str) -> int:
    name = ['ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС']
    for i in range(7):
        if weekday_name == name[i]:
            return i
    return -1

def is_group_chat(update: Update) -> bool:
    if (update is not None and
        update.message is not None and
        update.message.chat is not None and
        update.message.chat.type is not None and
        update.message.chat.type in [Chat.GROUP, Chat.SUPERGROUP]):
        return True
    return False