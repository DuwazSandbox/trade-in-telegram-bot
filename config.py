#!/usr/bin/env python3

import json

class Config():
    def __init__(self, config_path: str):
        self._pools = []
        with open(config_path, 'r', encoding='utf-8') as json_file:
            self._pools = json.load(json_file)

    def get_pools(self) -> list:
        return self._pools
