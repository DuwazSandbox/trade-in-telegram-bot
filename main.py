#!/usr/bin/env python3

from training.actions import TrainingActions

import logging
import os

from telegram.ext import CallbackQueryHandler, CommandHandler, Updater

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

CONFIG_PATH = 'training/data.json'

DATABASE_URL = os.environ.get('DATABASE_URL')
API_TOKEN = os.environ.get('API_TOKEN')

def main() -> None:
    try:
        TrainingActions.init(CONFIG_PATH, DATABASE_URL)
    except Exception:
        logger.critical('Cannot start')
        return

    updater = Updater(API_TOKEN)

    # Only for group chat
    updater.dispatcher.add_handler(CommandHandler('status', TrainingActions.GroupChat.status))

    # Only for user chat
    updater.dispatcher.add_handler(CommandHandler('start', TrainingActions.UserChat.start))
    updater.dispatcher.add_handler(CallbackQueryHandler(TrainingActions.UserChat.callback_button))

    # Start the Bot
    updater.start_polling()

    # Run the bot until the user presses Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT
    updater.idle()


if __name__ == '__main__':
    main()