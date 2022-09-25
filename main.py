#!/usr/bin/env python3

from actions import Actions
from database_error import DatabaseError

import os

from telegram.ext import Updater, CommandHandler, CallbackQueryHandler

CONFIG_PATH = 'trade-in.json'

DATABASE_URL = os.environ.get('DATABASE_URL')
API_TOKEN = os.environ.get('API_TOKEN')

def main() -> None:
    status = Actions.init(CONFIG_PATH, DATABASE_URL)
    if status != DatabaseError.Ok:
        print('Cannot start') # !!! use logger
        return

    updater = Updater(API_TOKEN)

    # Only for group chat
    updater.dispatcher.add_handler(CommandHandler('status', Actions.GroupChat.status))

    # Only for user chat
    updater.dispatcher.add_handler(CommandHandler('start', Actions.UserChat.start))
    updater.dispatcher.add_handler(CallbackQueryHandler(Actions.UserChat.callback_button))

    # Start the Bot
    updater.start_polling()

    # Run the bot until the user presses Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT
    updater.idle()


if __name__ == '__main__':
    main()