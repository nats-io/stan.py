# Copyright 2017 Apcera Inc. All rights reserved.

import asyncio

class StanError(Exception):
    pass

class ErrTimeoutError(asyncio.TimeoutError):
    pass

class ErrConnectReqTimeout(ErrTimeoutError):
    pass
