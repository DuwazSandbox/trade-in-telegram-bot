#!/usr/bin/env python3

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update

class KeyboardManager:
    def __init__(self, update: Update, text: str):
        self._update = update
        self._text = text
        self._show_button_home = True
        self._is_first_msg = False
        self._keyboard = []
        self._back_action = ''

    def add_button(self, text: str, action: str) -> None:
        data = InlineKeyboardButton(text, callback_data = action)
        if len(self._keyboard) == 0:
            self._keyboard = [[data]]
        elif len(self._keyboard[-1]) == 1:
            self._keyboard[-1].append(data)
        else:
            self._keyboard.append([data])

    def set_show_button_home(self, show_button_home: bool):
        self._show_button_home = show_button_home

    def set_is_first_msg(self, is_first_msg: bool):
        self._is_first_msg = is_first_msg

    def set_back_action(self, action: str):
        self._back_action = action

    def set_text(self, text: str) -> None:
        self._text = text

    def update(self) -> None:
        if self._show_button_home:
            additional_buttons = []
            additional_buttons.append(InlineKeyboardButton('В начало', callback_data = 'restart'))
            if len(self._back_action) != 0:
                additional_buttons.append(InlineKeyboardButton('Назад', callback_data = self._back_action))
            self._keyboard.append(additional_buttons)

        func = self._update.message.reply_text if self._is_first_msg else self._update.callback_query.message.edit_text
        func(self._text, reply_markup=InlineKeyboardMarkup(self._keyboard))

    @staticmethod
    def make_yes_no_dialog(yes: dict, no: dict) -> None:
        return InlineKeyboardMarkup([[
            InlineKeyboardButton(yes['text'], callback_data = yes['callback']),
            InlineKeyboardButton(no['text'], callback_data = no['callback'])
        ]])
