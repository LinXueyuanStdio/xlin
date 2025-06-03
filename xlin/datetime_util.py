

import datetime
import random


date_str = datetime.datetime.now().strftime("%Y%m%d")
datetime_str = datetime.datetime.now().strftime("%Y%m%d_%Hh%Mm%Ss")


def random_timestamp(start_timestamp=None, end_timestamp=None):
    if start_timestamp is None:
        start_timestamp = datetime.datetime(2024, 1, 1).timestamp()
    if end_timestamp is None:
        end_timestamp = datetime.datetime.now().timestamp()
    return random.uniform(start_timestamp, end_timestamp)


def random_timestamp_str(start_timestamp=None, end_timestamp=None, format="%Y年%m月%d日%H时%M分"):
    return datetime.datetime.fromtimestamp(random_timestamp(start_timestamp, end_timestamp)).strftime(format)

