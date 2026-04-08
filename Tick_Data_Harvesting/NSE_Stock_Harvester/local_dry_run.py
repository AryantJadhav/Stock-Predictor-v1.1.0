#!/usr/bin/env python3

import _thread
import threading
import time

import tick_harvester


def _auto_shutdown_after_60s() -> None:
    time.sleep(60)
    _thread.interrupt_main()


def main() -> None:
    tick_harvester.BATCH_SIZE = 1
    tick_harvester._is_trading_day = lambda d: True
    tick_harvester._is_token_fresh = lambda: True

    stopper = threading.Thread(target=_auto_shutdown_after_60s, daemon=True)
    stopper.start()

    tick_harvester.main()


if __name__ == "__main__":
    main()
