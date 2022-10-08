#!/usr/bin/env python3

from enum import IntEnum

class DatabaseError(IntEnum):
    Ok = 0
    InvalidData = 1
    RecordExists = 2
    InternalError = 3
    RecordUsed = 4