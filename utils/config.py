#!/usr/bin/env python3

import json

class Config():
    def __init__(self, config_path: str):
        self._data = []
        with open(config_path, 'r', encoding='utf-8') as json_file:
            self._data = json.load(json_file)

    def get_data(self) -> list:
        return self._data
