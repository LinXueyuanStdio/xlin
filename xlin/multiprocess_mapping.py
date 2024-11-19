import time
import os
import multiprocessing
from multiprocessing.pool import ThreadPool
from typing import *

import pandas as pd
from pathlib import Path
from tqdm import tqdm
from loguru import logger

from jsonl import dataframe_to_json_list, load_json_list, save_json_list, load_json, save_json


cpu_count = multiprocessing.cpu_count()
# pool = ThreadPool(cpu_count)  # 大模型接口辣鸡，太快会截断答案
thread_pool_size = int(os.getenv("THREAD_POOL_SIZE", 5))
pool = ThreadPool(thread_pool_size)
logger.debug(f"pool size: {thread_pool_size}, cpu count: {cpu_count}")


def multiprocessing_mapping_jsonlist(
    jsonlist: List[Any],
    output_path: Optional[Union[str, Path]],
    partial_func,
    batch_size=cpu_count * 2,
    cache_batch_num=1,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 5)),
):
    """mapping a column to another column

    Args:
        df (DataFrame): [description]
        output_path (Path): 数据量大的时候需要缓存
        partial_func (function): (Dict[str, str]) -> Dict[str, str]
    """
    need_caching = output_path is not None
    tmp_list, output_list = list(), list()
    start_idx = 0
    if need_caching:
        output_path = Path(output_path)
        if output_path.exists():
            output_list = load_json_list(output_path)
            start_idx = len(output_list)
            logger.warning(f"Cache found {output_path} has {start_idx} rows. This process will continue at row index {start_idx}.")
            logger.warning(f"缓存 {output_path} 存在 {start_idx} 行. 本次处理将从第 {start_idx} 行开始.")
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
    pool = ThreadPool(thread_pool_size)
    logger.debug(f"pool size: {thread_pool_size}, cpu count: {cpu_count}")
    start_time = time.time()
    last_save_time = start_time
    for i, line in tqdm(list(enumerate(jsonlist))):
        if i < start_idx:
            continue
        tmp_list.append(line)
        if len(tmp_list) == batch_size:
            results = pool.map(partial_func, tmp_list)
            output_list.extend([x for x in results])
            tmp_list = list()
        if need_caching and (i // batch_size) % cache_batch_num == 0:
            current_time = time.time()
            if current_time - last_save_time < 3:
                # 如果多进程处理太快，为了不让 IO 成为瓶颈拉慢进度，不足 3 秒的批次都忽略，也不缓存中间结果
                last_save_time = current_time
                continue
            save_json_list(output_list, output_path)
            last_save_time = time.time()
    if len(tmp_list) > 0:
        results = pool.map(partial_func, tmp_list)
        output_list.extend([x for x in results])
    if need_caching:
        save_json_list(output_list, output_path)
    return output_list


def multiprocessing_mapping(
    df: pd.DataFrame,
    output_path: Optional[Union[str, Path]],
    partial_func,
    batch_size=cpu_count * 2,
    cache_batch_num=1,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 5)),
):
    """mapping a column to another column

    Args:
        df (DataFrame): [description]
        output_path (Path): 数据量大的时候需要缓存
        partial_func (function): (Dict[str, str]) -> Dict[str, str]
    """
    need_caching = output_path is not None
    tmp_list, output_list = list(), list()
    start_idx = 0
    if need_caching:
        output_path = Path(output_path)
        if output_path.exists():
            # existed_df = read_as_dataframe(output_path)
            # start_idx = len(existed_df)
            # output_list = dataframe_to_json_list(existed_df)
            # logger.warning(f"Cache found {output_path} has {start_idx} rows. This process will continue at row index {start_idx}.")
            # logger.warning(f"缓存 {output_path} 存在 {start_idx} 行. 本次处理将从第 {start_idx} 行开始.")
            pass
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
    pool = ThreadPool(thread_pool_size)
    logger.debug(f"pool size: {thread_pool_size}, cpu count: {cpu_count}")
    start_time = time.time()
    last_save_time = start_time
    for i, line in tqdm(list(df.iterrows())):
        if i < start_idx:
            continue
        line_info: dict = line.to_dict()
        line_info: Dict[str, str] = {str(k): str(v) for k, v in line_info.items()}
        tmp_list.append(line_info)
        if len(tmp_list) == batch_size:
            results = pool.map(partial_func, tmp_list)
            output_list.extend([x for x in results])
            tmp_list = list()
        if need_caching and (i // batch_size) % cache_batch_num == 0:
            current_time = time.time()
            if current_time - last_save_time < 3:
                # 如果多进程处理太快，为了不让 IO 成为瓶颈拉慢进度，不足 3 秒的批次都忽略，也不缓存中间结果
                last_save_time = current_time
                continue
            output_df = pd.DataFrame(output_list)
            output_df.to_excel(output_path, index=False)
            last_save_time = time.time()
    if len(tmp_list) > 0:
        results = pool.map(partial_func, tmp_list)
        output_list.extend([x for x in results])
    output_df = pd.DataFrame(output_list)
    if need_caching:
        output_df.to_excel(output_path, index=False)
    return output_df, output_list


def dataframe_by_row_mapping(
    df: pd.DataFrame,
    mapping_func: Callable[[dict], Tuple[bool, dict]],
    use_multiprocessing=True,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 5)),
):
    rows = []
    if use_multiprocessing:
        pool = ThreadPool(thread_pool_size)
        logger.debug(f"pool size: {thread_pool_size}, cpu count: {cpu_count}")
        results = pool.map(mapping_func, dataframe_to_json_list(df))
        for ok, row in results:
            if ok:
                rows.append(row)
    else:
        for i, row in df.iterrows():
            ok, row = mapping_func(row)
            if ok:
                rows.append(row)
    df = pd.DataFrame(rows)
    return df


def continue_run(
    jsonfiles: List[str],
    save_dir: str,
    mapping_func,
    load_func=load_json,
    save_func=save_json,
    batch_size=1024,
    cache_size=8,
):
    save_dir: Path = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    new_jsonfiles = []
    for jsonfile in jsonfiles:
        jsonfile = Path(jsonfile)
        jsonlist = load_func(jsonfile)
        output_filepath = save_dir / jsonfile.name
        for row in jsonlist:
            row["来源"] = jsonfile.name
        new_jsonlist = multiprocessing_mapping_jsonlist(
            jsonlist,
            output_filepath,
            mapping_func,
            batch_size,
            cache_size,
        )
        save_func(new_jsonlist, output_filepath)
        new_jsonfiles.append(output_filepath)
    return new_jsonfiles
