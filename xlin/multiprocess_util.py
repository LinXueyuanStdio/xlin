import time
import os
import multiprocessing
from multiprocessing.pool import ThreadPool
from typing import *

import pandas as pd
from pathlib import Path
from tqdm import tqdm
from loguru import logger

from xlin.jsonlist_util import append_to_json_list, dataframe_to_json_list, load_json_list, row_to_json, save_json_list, load_json, save_json
from xlin.dataframe_util import read_as_dataframe
from xlin.file_util import ls


def element_mapping(
    iterator: list[Any],
    mapping_func: Callable[[Any], Tuple[bool, Any]],
    use_multiprocessing=True,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 5)),
):
    rows = []
    # 转换为列表以获取长度，用于进度条显示
    items = list(iterator)
    total = len(items)

    if use_multiprocessing:
        pool = ThreadPool(thread_pool_size)
        # 使用imap替代map，结合tqdm显示进度
        for ok, row in tqdm(pool.imap(mapping_func, items), total=total, desc="Processing"):
            if ok:
                rows.append(row)
        pool.close()
    else:
        for row in tqdm(items, desc="Processing"):
            ok, row = mapping_func(row)
            if ok:
                rows.append(row)
    return rows


def batch_mapping(
    iterator: list[Any],
    mapping_func: Callable[[list[Any]], Tuple[bool, list[Any]]],
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


def xmap(
    jsonlist: list[Any],
    work_func: Union[Callable[[Any], dict], Callable[[list[Any]], list[dict]]],
    output_path: Optional[Union[str, Path]]=None,  # 输出路径，None表示不缓存
    batch_size=multiprocessing.cpu_count(),
    cache_batch_num=1,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 8)),
    use_process_pool=True,  # CPU密集型任务时设为True
    preserve_order=True,  # 是否保持结果顺序
    chunksize=None,  # 自动计算最佳分块大小
    retry_count=0,  # 失败重试次数
    force_overwrite=False,  # 是否强制覆盖输出文件
    is_batch_work_func=False,  # 是否批量处理函数
    verbose=False,  # 是否打印详细信息
):
    """高效处理JSON列表，支持多进程/多线程

    Args:
        jsonlist (list[Any]): 要处理的JSON对象列表
        output_path (Optional[Union[str, Path]]): 输出路径，None表示不缓存
        work_func (Callable): 处理函数，接收dict返回dict
        batch_size (int): 批处理大小
        cache_batch_num (int): 缓存批次数量
        thread_pool_size (int): 线程/进程池大小
        use_process_pool (bool): 是否使用进程池(CPU密集型任务)
        preserve_order (bool): 是否保持结果顺序
        chunksize (Optional[int]): 单个任务分块大小，None为自动计算
        retry_count (int): 任务失败重试次数

    Example:
        >>> from xlin.multiprocess_mapping import xmap
        >>> jsonlist = [{"id": 1, "text": "Hello"}, {"id": 2, "text": "World"}]
        >>> def work_func(item):
        ...     item["text"] = item["text"].upper()
        ...     return item
        >>> results = xmap(jsonlist, work_func, output_path="output.jsonl", batch_size=2)
        >>> print(results)
        [{'id': 1, 'text': 'HELLO'}, {'id': 2, 'text': 'WORLD'}]
    """
    need_caching = output_path is not None
    output_list = []
    start_idx = 0

    # 自动计算最佳chunksize
    if chunksize is None:
        chunksize = max(1, min(batch_size // thread_pool_size, 100))

    # 处理缓存
    if need_caching:
        output_path = Path(output_path)
        if output_path.exists():
            if force_overwrite:
                if verbose:
                    logger.warning(f"强制覆盖输出文件: {output_path}")
                output_path.unlink()
            else:
                output_list = load_json_list(output_path)
                start_idx = len(output_list)
                if verbose:
                    logger.info(f"继续处理: 已有{start_idx}条记录，共{len(jsonlist)}条")
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)

    # 选择线程池或进程池
    if use_process_pool:
        pool_cls = multiprocessing.Pool
        if verbose:
            logger.info(f"使用进程池(ProcessPool)，适用于CPU密集型任务")
    else:
        pool_cls = ThreadPool
        if verbose:
            logger.info(f"使用线程池(ThreadPool)，适用于IO密集型任务")

    with pool_cls(thread_pool_size) as pool:
        if verbose:
            logger.info(f"池大小: {thread_pool_size}, 批处理大小: {batch_size}, 分块大小: {chunksize}")

        # 准备要处理的数据
        remaining_items = jsonlist[start_idx:]
        total_items = len(remaining_items)

        # 批量处理逻辑
        def process_batch(items_batch, retry_remaining=retry_count):
            try:
                if is_batch_work_func:
                    # 批量处理函数
                    return work_func(items_batch)
                else:
                    # 选择合适的映射方法
                    map_func = pool.imap_unordered if not preserve_order else pool.imap
                    return list(map_func(work_func, items_batch, chunksize))
            except Exception as e:
                if retry_remaining > 0:
                    if verbose:
                        logger.warning(f"批处理失败，重试中 ({retry_count-retry_remaining+1}/{retry_count}): {e}")
                    return process_batch(items_batch, retry_remaining - 1)
                else:
                    if verbose:
                        logger.error(f"批处理失败: {e}")
                    raise

        # 处理数据
        with tqdm(total=total_items, desc="Map", unit="examples") as pbar:
            # 跳过已处理的项目
            pbar.update(start_idx)

            # 分批处理
            for i in range(0, total_items, batch_size):
                batch = remaining_items[i : i + batch_size]

                # 处理当前批次
                batch_start_time = time.time()
                results = process_batch(batch)
                batch_time = time.time() - batch_start_time

                # 更新结果
                output_list.extend(results)
                pbar.update(len(batch))

                # 性能统计
                pbar.set_postfix_str(f"{batch_time:.1f} s/batch, {len(results)} examples")

                # 缓存逻辑
                if need_caching and (i // batch_size) % cache_batch_num == 0:
                    # 仅当处理速度足够慢时才保存缓存，避免IO成为瓶颈
                    if batch_time > 3 or i + batch_size >= total_items:
                        save_json_list(output_list, output_path)
                        logger.debug(f"已保存 {len(output_list)} 条记录到 {output_path}")

    # 最终保存
    if need_caching:
        save_json_list(output_list, output_path)
    if verbose:
        drop_count = len(jsonlist) - len(output_list)
        logger.info(f"处理完成，共处理 {len(jsonlist)} 条记录" + f", 丢弃 {drop_count} 条记录" if drop_count > 0 else "")

    return output_list


def multiprocessing_mapping(
    df: pd.DataFrame,
    output_path: Optional[Union[str, Path]],
    partial_func: Callable[[Dict[str, str]], Dict[str, str]],
    batch_size=multiprocessing.cpu_count(),
    cache_batch_num=1,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 5)),
):
    """mapping a column to another column

    Args:
        df (DataFrame): [description]
        output_path (Path): 数据量大的时候需要缓存
        partial_func (function): (Dict[str, str]) -> Dict[str, str]
        batch_size (int): batch size
        cache_batch_num (int): cache batch num
        thread_pool_size (int): thread pool size
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
