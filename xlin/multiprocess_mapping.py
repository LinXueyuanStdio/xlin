import time
import os
import multiprocessing
from multiprocessing.pool import ThreadPool
from typing import *

import pandas as pd
from pathlib import Path
from tqdm import tqdm
from loguru import logger

from xlin.jsonl import append_to_json_list, dataframe_to_json_list, load_json_list, row_to_json, save_json_list, load_json, save_json
from xlin.read_as_dataframe import read_as_dataframe
from xlin.util import ls


def element_mapping(
    iterator: List[Any],
    mapping_func: Callable[[Any], Tuple[bool, Any]],
    use_multiprocessing=True,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 5)),
):
    rows = []
    if use_multiprocessing:
        pool = ThreadPool(thread_pool_size)
        results = pool.map(mapping_func, iterator)
        pool.close()
        for ok, row in results:
            if ok:
                rows.append(row)
    else:
        for row in tqdm(iterator):
            ok, row = mapping_func(row)
            if ok:
                rows.append(row)
    return rows


def batch_mapping(
    iterator: List[Any],
    mapping_func: Callable[[List[Any]], Tuple[bool, List[Any]]],
    use_multiprocessing=True,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 5)),
    batch_size=4,
):
    batch_iterator = []
    batch = []
    for i, item in enumerate(iterator):
        batch.append(item)
        if len(batch) == batch_size:
            batch_iterator.append(batch)
            batch = []
    if len(batch) > 0:
        batch_iterator.append(batch)
    rows = element_mapping(batch_iterator, mapping_func, use_multiprocessing, thread_pool_size)
    rows = [row for batch in rows for row in batch]
    return rows


def dataframe_with_row_mapping(
    df: pd.DataFrame,
    mapping_func: Callable[[dict], Tuple[bool, dict]],
    use_multiprocessing=True,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 5)),
):
    rows = element_mapping(df.iterrows(), lambda x: mapping_func(x[1]), use_multiprocessing, thread_pool_size)
    df = pd.DataFrame(rows)
    return df


def multiprocessing_mapping_jsonlist(
    jsonlist: List[Any],
    output_path: Optional[Union[str, Path]],
    partial_func,
    batch_size=multiprocessing.cpu_count(),
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
    logger.debug(f"pool size: {thread_pool_size}, cpu count: {multiprocessing.cpu_count()}")
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
    pool.close()
    if need_caching:
        save_json_list(output_list, output_path)
    return output_list


def multiprocessing_mapping(
    df: pd.DataFrame,
    output_path: Optional[Union[str, Path]],
    partial_func,
    batch_size=multiprocessing.cpu_count(),
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
    logger.debug(f"pool size: {thread_pool_size}, cpu count: {multiprocessing.cpu_count()}")
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
    pool.close()
    output_df = pd.DataFrame(output_list)
    if need_caching:
        output_df.to_excel(output_path, index=False)
    return output_df, output_list


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
    for jsonfile in ls(jsonfiles):
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


def dataframe_mapping(
    df: pd.DataFrame,
    row_func: Callable[[dict], dict],
    output_path: Optional[Union[str, Path]] = None,
    force_overwrite: bool = False,
    batch_size=multiprocessing.cpu_count(),
    cache_batch_num=1,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 5)),
):
    """mapping a column to another column

    Args:
        df (DataFrame): [description]
        row_func (function): (Dict[str, str]) -> Dict[str, str]
        output_path (Path): 数据量大的时候需要缓存. None 表示不缓存中间结果
        force_overwrite (bool): 是否强制覆盖 output_path
        batch_size (int): batch size
        cache_batch_num (int): cache batch num
        thread_pool_size (int): thread pool size
    """
    need_caching = output_path is not None
    tmp_list, output_list = list(), list()
    start_idx = 0
    if need_caching:
        output_path = Path(output_path)
        if output_path.exists() and not force_overwrite:
            existed_df = read_as_dataframe(output_path)
            start_idx = len(existed_df)
            output_list = dataframe_to_json_list(existed_df)
            logger.warning(f"Cache found that {output_path} has {start_idx} rows. This process will continue at row index {start_idx}.")
            logger.warning(f"缓存 {output_path} 存在 {start_idx} 行. 本次处理将从第 {start_idx} 行开始.")
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
    pool = ThreadPool(thread_pool_size)
    logger.debug(f"pool size: {thread_pool_size}, cpu count: {multiprocessing.cpu_count()}")
    start_time = time.time()
    last_save_time = start_time
    with tqdm(total=len(df), desc="Processing", unit="rows") as pbar:
        for i, line in df.iterrows():
            pbar.update(1)
            if i < start_idx:
                continue
            line_info: dict = line.to_dict()
            tmp_list.append(line_info)
            if len(tmp_list) == batch_size:
                results = pool.map(row_func, tmp_list)
                output_list.extend([row_to_json(x) for x in results])
                tmp_list = list()
            if need_caching and (i // batch_size) % cache_batch_num == 0:
                current_time = time.time()
                if current_time - last_save_time < 3:
                    # 如果多进程处理太快，为了不让 IO 成为瓶颈拉慢进度，不足 3 秒的批次都忽略，也不缓存中间结果
                    last_save_time = current_time
                    continue
                rows_to_cache = output_list[start_idx:]
                append_to_json_list(rows_to_cache, output_path)
                start_idx = len(output_list)
                last_save_time = time.time()
            if need_caching:
                pbar.set_postfix_str(f"Cache: {len(output_list)}/{len(df)}")
        if len(tmp_list) > 0:
            results = pool.map(row_func, tmp_list)
            output_list.extend([row_to_json(x) for x in results])
        pool.close()
        if need_caching:
            rows_to_cache = output_list[start_idx:]
            append_to_json_list(rows_to_cache, output_path)
            start_idx = len(output_list)
            pbar.set_postfix_str(f"Cache: {len(output_list)}/{len(df)}")
    output_df = pd.DataFrame(output_list)
    return output_df


def dataframe_batch_mapping(
    df: pd.DataFrame,
    batch_row_func: Callable[[list[dict]], dict],
    output_path: Optional[Union[str, Path]] = None,
    force_overwrite: bool = False,
    batch_size=multiprocessing.cpu_count(),
    cache_batch_num=1,
):
    """mapping a column to another column

    Args:
        df (DataFrame): [description]
        row_func (function): (Dict[str, str]) -> Dict[str, str]
        output_path (Path): 数据量大的时候需要缓存. None 表示不缓存中间结果
        force_overwrite (bool): 是否强制覆盖 output_path
        batch_size (int): batch size
        cache_batch_num (int): cache batch num
        thread_pool_size (int): thread pool size
    """
    need_caching = output_path is not None
    tmp_list, output_list = list(), list()
    start_idx = 0
    if need_caching:
        output_path = Path(output_path)
        if output_path.exists() and not force_overwrite:
            existed_df = read_as_dataframe(output_path)
            start_idx = len(existed_df)
            output_list = dataframe_to_json_list(existed_df)
            logger.warning(f"Cache found that {output_path} has {start_idx} rows. This process will continue at row index {start_idx}.")
            logger.warning(f"缓存 {output_path} 存在 {start_idx} 行. 本次处理将从第 {start_idx} 行开始.")
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
    start_time = time.time()
    last_save_time = start_time
    with tqdm(total=len(df), desc="Processing", unit="rows") as pbar:
        for i, line in df.iterrows():
            pbar.update(1)
            if i < start_idx:
                continue
            line_info: dict = line.to_dict()
            tmp_list.append(line_info)
            if len(tmp_list) == batch_size:
                results = batch_row_func(tmp_list)
                output_list.extend([row_to_json(x) for x in results])
                tmp_list = list()
            if need_caching and (i // batch_size) % cache_batch_num == 0:
                current_time = time.time()
                if current_time - last_save_time < 3:
                    # 如果多进程处理太快，为了不让 IO 成为瓶颈拉慢进度，不足 3 秒的批次都忽略，也不缓存中间结果
                    last_save_time = current_time
                    continue
                rows_to_cache = output_list[start_idx:]
                append_to_json_list(rows_to_cache, output_path)
                start_idx = len(output_list)
                last_save_time = time.time()
            if need_caching:
                pbar.set_postfix_str(f"Cache: {len(output_list)}/{len(df)}")
        if len(tmp_list) > 0:
            results = batch_row_func(tmp_list)
            output_list.extend([row_to_json(x) for x in results])
        if need_caching:
            rows_to_cache = output_list[start_idx:]
            append_to_json_list(rows_to_cache, output_path)
            start_idx = len(output_list)
            pbar.set_postfix_str(f"Cache: {len(output_list)}/{len(df)}")
    output_df = pd.DataFrame(output_list)
    return output_df
