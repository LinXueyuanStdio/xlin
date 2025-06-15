import asyncio
import time
import os
import multiprocessing
from multiprocessing.pool import ThreadPool
from typing_extensions import *

import pandas as pd
from pathlib import Path
from tqdm import tqdm
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Executor
from tqdm.asyncio import tqdm as asyncio_tqdm
import heapq

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


async def xmap_async(
    jsonlist: list[Any],
    work_func: Union[
      Callable[[Any], dict],
      Callable[[list[Any]], list[dict]],
      Awaitable[Callable[[Any], dict]],
      Awaitable[Callable[[list[Any]], list[dict]]],
    ],
    output_path: Optional[Union[str, Path]] = None,
    *,
    desc: str = "Processing",
    max_workers=8,  # 最大工作线程数
    use_process_pool=True,  # CPU密集型任务时设为True
    preserve_order=True,  # 是否保持结果顺序
    retry_count=0,  # 失败重试次数
    force_overwrite=False,  # 是否强制覆盖输出文件
    is_batch_work_func=False,  # 是否批量处理函数
    batch_size=32,  # 批量处理大小
    is_async_work_func=False,  # 是否异步函数
    verbose=False,  # 是否打印详细信息
):
    """xmap_async 是 xmap 的异步版本，使用 async/await 实现高性能并发处理。
    特别适用于 I/O 密集型任务，如网络请求、文件操作等。支持处理过程中的实时缓存。

    Args:
        jsonlist (list[Any]): 要处理的JSON对象列表
        work_func (Callable): 处理函数，可以是同步或异步的
            - 同步单个处理函数 (item) -> Dict
            - 同步批量处理函数 (List[item]) -> List[Dict]
            - 异步单个处理函数 async (item) -> Dict
            - 异步批量处理函数 async (List[item]) -> List[Dict]
            使用批量处理函数时，`is_batch_work_func` 参数必须设置为 `True`。内部会自动按 `batch_size` 切分数据。
        output_path (Optional[Union[str, Path]]): 输出路径，None表示不缓存
        desc (str): 进度条描述
        max_workers (int): 最大工作线程数，默认为8
        use_process_pool (bool): 是否使用进程池，默认为True
        preserve_order (bool): 是否保持结果顺序，默认为True
        retry_count (int): 失败重试次数，默认为0
        force_overwrite (bool): 是否强制覆盖输出文件，默认为False
        is_batch_work_func (bool): 是否批量处理函数，默认为False
        batch_size (int): 批量处理大小，默认为32. 仅当`is_batch_work_func`为True时有效
        is_async_work_func (bool): 是否异步函数，默认为False
        verbose (bool): 是否打印详细信息，默认为False

    Returns:
        list[Any]: 处理后的结果列表，包含原始数据和处理结果

    Examples:
        1. 同步单个处理函数:
            ```python
            def process_item(item):
                # 处理单个项目
                return {"id": item["id"], "value": item["value"] * 2}

            results = await xmap_async(jsonlist, process_item)
            ```
        2. 同步批量处理函数:
            ```python
            def process_batch(items):
                # 处理批量项目
                return [{"id": item["id"], "value": item["value"] * 2} for item in items]

            results = await xmap_async(jsonlist, process_batch, is_batch_work_func=True)
            ```
        3. 异步单个处理函数:
            ```python
            async def async_process_item(item):
            # 异步处理单个项目
            await asyncio.sleep(0.1)  # 模拟异步操作
            return {"id": item["id"], "value": item["value"] * 2}

            results = await xmap_async(jsonlist, async_process_item, is_async_work_func=True)
            ```
        4. 异步批量处理函数:
            ```python
            async def async_process_batch(items):
                # 异步处理批量项目
                return await asyncio.gather(*[async_process_item(item) for item in items])  # 模拟异步操作

            results = await xmap_async(jsonlist, async_process_batch, is_async_work_func=True, is_batch_work_func=True)
            ```
    """
    need_caching = output_path is not None
    output_list = []
    start_idx = 0

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

    # 准备要处理的数据
    remaining = jsonlist[start_idx:]
    if is_batch_work_func:
        remaining = [remaining[i:i + batch_size] for i in range(0, len(remaining), batch_size)]

    loop = asyncio.get_event_loop()
    if use_process_pool:
        executor: Executor = ProcessPoolExecutor(max_workers=max_workers)
    else:
        executor = ThreadPoolExecutor(max_workers=max_workers)

    async def submit_task(index: int, item: Any):
        if is_async_work_func:
            return index, await work_func(item)
        return index, await loop.run_in_executor(executor, work_func, item)

    # 异步调度
    results = []
    pq = []

    async def schedule_items():
        sem = asyncio.Semaphore(max_workers)
        pbar = asyncio_tqdm(total=len(remaining), desc=desc, unit="it")
        result_queue = asyncio.Queue()

        async def task_fn(index: int, item: Any | list[Any]):
            async with sem:
                # 实现重试逻辑
                for retry_step_idx in range(retry_count + 1):
                    try:
                        result = await submit_task(index, item)
                        await result_queue.put(result)
                        break
                    except Exception as e:
                        if retry_step_idx < retry_count:
                            if verbose:
                                logger.error(f"处理失败，索引 {index} 重试中 ({retry_step_idx + 1}/{retry_count}): {e}")
                        else:
                            if verbose:
                                logger.error(f"最终失败，无法处理索引 {index} 的项目: {e}")
                            fallback_result = {"index": index, "error": str(e)}
                            if is_batch_work_func:
                                fallback_result = [fallback_result] * batch_size
                            # 将错误结果放入队列
                            await result_queue.put((index, fallback_result))


        async def producer():
            for i, item in enumerate(remaining):
                index = i + start_idx
                asyncio.create_task(task_fn(index, item))

        asyncio.create_task(producer())

        next_expect = start_idx

        while len(results) + start_idx < len(jsonlist):
            idx, res = await result_queue.get()

            if preserve_order:
                heapq.heappush(pq, (idx, res))
                # 保序输出
                output_buffer = []
                while pq and pq[0][0] == next_expect:
                    _, r = heapq.heappop(pq)
                    if is_batch_work_func:
                        output_buffer.extend(r)
                        results.extend(r)
                    else:
                        output_buffer.append(r)
                        results.append(r)
                    next_expect += 1
                    pbar.update(1)
                if need_caching:
                    append_to_json_list(output_buffer, output_path)
            else:
                # 非保序输出
                if is_batch_work_func:
                    results.extend(res)
                    if need_caching:
                        append_to_json_list(res, output_path)
                else:
                    results.append(res)
                    if need_caching:
                        append_to_json_list([res], output_path)
                pbar.update(1)

        pbar.close()

    await schedule_items()
    return jsonlist[:start_idx] + results

def xmap(
    jsonlist: list[Any],
    work_func: Union[
        Callable[[Any], dict],
        Callable[[list[Any]], list[dict]],
        Awaitable[Callable[[Any], dict]],
        Awaitable[Callable[[list[Any]], list[dict]]],
    ],
    output_path: Optional[Union[str, Path]]=None,  # 输出路径，None表示不缓存
    *,
    desc: str = "Processing",
    max_workers=8,  # 最大工作线程数
    use_process_pool=True,  # CPU密集型任务时设为True
    preserve_order=True,  # 是否保持结果顺序
    retry_count=0,  # 失败重试次数
    force_overwrite=False,  # 是否强制覆盖输出文件
    is_batch_work_func=False,  # 是否批量处理函数
    batch_size=8,  # 批量处理大小，仅当`is_batch_work_func`为True时有效
    is_async_work_func=False,  # 是否异步处理函数
    verbose=False,  # 是否打印详细信息
):
    """高效处理JSON列表，支持多进程/多线程

    Args:
        jsonlist (List[Any]): 需要处理的JSON数据列表
        work_func (Callable): 处理函数，可以是同步或异步的
            - 同步单个处理函数 (item) -> Dict
            - 同步批量处理函数 (List[item]) -> List[Dict]
            - 异步单个处理函数 async (item) -> Dict
            - 异步批量处理函数 async (List[item]) -> List[Dict]
            使用批量处理函数时，`is_batch_work_func` 参数必须设置为 `True`。内部会自动按 `batch_size` 切分数据。
        output_path (Optional[Union[str, Path]]): 输出路径，且同时是实时缓存路径。None表示不缓存，结果仅保留在内存中
        desc (str): 进度条描述
        max_workers (int): 最大工作线程数，默认为8
        use_process_pool (bool): 是否使用进程池，默认为True
        preserve_order (bool): 是否保持结果顺序，默认为True
        retry_count (int): 失败重试次数，默认为0
        force_overwrite (bool): 是否强制覆盖输出文件，默认为False
        is_batch_work_func (bool): 是否批量处理函数，默认为False
        batch_size (int): 批量处理大小，默认为32. 仅当`is_batch_work_func`为True时有效
        is_async_work_func (bool): 是否异步函数，默认为False
        verbose (bool): 是否打印详细信息，默认为False

    Returns:
        list[Any]: 处理后的结果列表，包含原始数据和处理结果

    Examples:
        1. 同步单个处理函数:
            ```python
            def process_item(item):
                # 处理单个项目
                return {"id": item["id"], "value": item["value"] * 2}

            results = xmap(jsonlist, process_item)
            ```
        2. 同步批量处理函数:
            ```python
            def process_batch(items):
                # 处理批量项目
                return [{"id": item["id"], "value": item["value"] * 2} for item in items]

            results = xmap(jsonlist, process_batch, is_batch_work_func=True)
            ```
        3. 异步单个处理函数:
            ```python
            async def async_process_item(item):
            # 异步处理单个项目
            await asyncio.sleep(0.1)  # 模拟异步操作
            return {"id": item["id"], "value": item["value"] * 2}

            results = xmap(jsonlist, async_process_item, is_async_work_func=True)
            ```
        4. 异步批量处理函数:
            ```python
            async def async_process_batch(items):
                # 异步处理批量项目
                return await asyncio.gather(*[async_process_item(item) for item in items])  # 模拟异步操作

            results = xmap(jsonlist, async_process_batch, is_async_work_func=True, is_batch_work_func=True)
            ```
    """
    return asyncio.run(
        xmap_async(
            jsonlist=jsonlist,
            work_func=work_func,
            output_path=output_path,
            desc=desc,
            max_workers=max_workers,
            use_process_pool=use_process_pool,
            preserve_order=preserve_order,
            retry_count=retry_count,
            force_overwrite=force_overwrite,
            is_batch_work_func=is_batch_work_func,
            batch_size=batch_size,
            is_async_work_func=is_async_work_func,
            verbose=verbose,
        )
    )



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


if __name__ == "__main__":
    jsonlist = [{"id": i, "text": "Hello World"} for i in range(1000)]
    def work_func(item):
        item["text"] = item["text"].upper()
        return item
    results = xmap(jsonlist, work_func, output_path="output.jsonl", batch_size=2)
    print(results)