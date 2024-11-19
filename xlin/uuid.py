from typing import *
import uuid

import pandas as pd


def append_uuid_column(df: pd.DataFrame, uuid_key="uuid"):
    rows = []
    for i, row in df.iterrows():
        row[uuid_key] = str(uuid.uuid4())
        rows.append(row)
    df = pd.DataFrame(rows)
    return df
