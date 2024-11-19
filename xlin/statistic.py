from typing import List

import pandas as pd



def bucket_count(length: List[int], step=50, skip_zero_count=False):
    grouped_count = []
    j = 0
    for i in range(0, max(length) + step, step):
        grouped_count.append(0)
        while j < len(length) and length[j] < i:
            grouped_count[i // step] += 1
            j += 1
    x, y = [], []
    for i, j in enumerate(grouped_count):
        if i == 0:
            continue
        if skip_zero_count and j == 0:
            continue
        print(f"[{(i-1)*step}, {i*step})  {j}   {sum(grouped_count[:i+1])/len(length)*100:.2f}%")
        x.append((i - 1) * step)
        y.append(j)
    return x, y


def statistic_char_length(df: pd.DataFrame, instruction_key="instruction"):
    length = []
    for i, row in df.iterrows():
        length.append(len(row[instruction_key]))
    length.sort()
    return length

