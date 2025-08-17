import asyncio
import os
from typing_extensions import *
from multiprocessing.pool import ThreadPool

from pathlib import Path
from tqdm import tqdm
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from tqdm.asyncio import tqdm as asyncio_tqdm
import heapq

from xlin.jsonlist_util import load_json_list, load_json, save_to_cache
from xlin.file_util import ls, rm


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
    cache_id: str = "uuid",  # 用于唯一标识处理结果的键，用于缓存
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
        cache_id (str): 用于唯一标识处理结果的键，用于缓存，默认为"uuid"

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
    output_list: list[dict] = []
    start_idx = 0

    # 处理缓存
    if need_caching:
        if not preserve_order:
            # 不保序时，缓存依赖于 cache_id 来跟踪缓存进度，必须保证每个 item 的 cache_id 唯一
            assert cache_id is not None, "缓存时必须提供唯一标识符来跟踪缓存进度"
            assert all(item.get(cache_id) is not None for item in jsonlist), "所有项都必须包含唯一标识符"
            assert len(set(item.get(cache_id) for item in jsonlist)) == len(jsonlist), "所有项的唯一标识符必须唯一，避免冲突"
        output_path = Path(output_path)
        if output_path.exists():
            if force_overwrite:
                if output_path.is_file():
                    if verbose:
                        logger.warning(f"强制覆盖输出文件: {output_path}")
                    output_path.unlink()
                else:
                    if verbose:
                        logger.warning(f"强制覆盖输出目录: {output_path}")
                    rm(output_path)
            else:
                if output_path.is_file():
                    output_list = load_json_list(output_path)
                    start_idx = len(output_list)
                    if not preserve_order:
                        # 如果不需要保序输出，则按 output_list 将已经处理的项从 jsonlist 中移动到前面，确保 start_idx 之后的项为未处理项
                        processed_ids = {item.get(cache_id) for item in output_list}
                        jsonlist_with_new_order = []
                        for item in jsonlist:
                            item_id = item.get(cache_id)
                            if item_id in processed_ids:
                                # 已处理的项放到前面
                                jsonlist_with_new_order.insert(0, item)
                            else:
                                # 未处理的项放到后面
                                jsonlist_with_new_order.append(item)
                        jsonlist = jsonlist_with_new_order
                else:
                    files = ls(output_path, filter=lambda f: f.name.endswith(".json"))
                    id2path = {f.name[:-5]: f for f in files}
                    jsonlist_with_new_order = []
                    for item in jsonlist:
                        item_id = item.get(cache_id)
                        if not preserve_order:
                            if item_id in processed_ids:
                                # 已处理的项放到前面
                                jsonlist_with_new_order.insert(0, item)
                            else:
                                # 未处理的项放到后面
                                jsonlist_with_new_order.append(item)
                        if item_id in id2path:
                            item_cache_path = id2path[item_id]
                            output_list.append(load_json(item_cache_path))
                            start_idx += 1
                        else:
                            if preserve_order:
                                # 如果需要保序输出，但缓存中没有该项，则跳过
                                break
                            # 如果不需要保序输出，则可以继续处理
                            output_list.append(item)
                            start_idx += 1
                    if not preserve_order:
                        jsonlist = jsonlist_with_new_order

                if start_idx >= len(jsonlist):
                    return output_list
                if verbose:
                    logger.info(f"继续处理: 已有{start_idx}条记录，共{len(jsonlist)}条")
        else:
            if output_path.name.endswith(".json") or output_path.name.endswith(".jsonl"):
                output_path.parent.mkdir(parents=True, exist_ok=True)
            else:
                output_path.mkdir(parents=True, exist_ok=True)

    # 准备要处理的数据
    remaining = jsonlist[start_idx:]
    if is_batch_work_func:
        remaining = [remaining[i:i + batch_size] for i in range(0, len(remaining), batch_size)]

    if not is_async_work_func:
        loop = asyncio.get_event_loop()
        if use_process_pool:
            executor = ProcessPoolExecutor(max_workers=max_workers)
        else:
            executor = ThreadPoolExecutor(max_workers=max_workers)

    async def working_at_task(index: int, item: Any):
        if is_async_work_func:
            return index, await work_func(item)
        return index, await loop.run_in_executor(executor, work_func, item)

    # 异步调度
    results: list[dict] = []
    pq = []
    sem = asyncio.Semaphore(max_workers)
    pbar = asyncio_tqdm(total=len(remaining), desc=desc, unit="it")
    result_queue = asyncio.Queue()

    async def task_fn(index: int, item: Any | list[Any]):
        # 实现重试逻辑
        for retry_step_idx in range(retry_count + 1):
            if verbose:
                print(f"Processing item at index {index}..." + ("" if retry_step_idx == 0 else f" (retry {retry_step_idx})"))
            async with sem:
                try:
                    result = await working_at_task(index, item)
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
        tasks = []
        for i, item in enumerate(remaining):
            index = i + start_idx
            task = asyncio.create_task(task_fn(index, item))
            tasks.append(task)
        await asyncio.gather(*tasks)

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
                else:
                    output_buffer.append(r)
                next_expect += 1
            if output_buffer:
                results.extend(output_buffer)
                pbar.update(len(output_buffer))
                if need_caching:
                    save_to_cache(output_buffer, output_path, cache_id, verbose)
        else:
            # 非保序输出
            output_buffer = []
            if is_batch_work_func:
                output_buffer.extend(res)
            else:
                output_buffer.append(res)
            if output_buffer:
                results.extend(output_buffer)
                pbar.update(len(output_buffer))
                if need_caching:
                    save_to_cache(output_buffer, output_path, cache_id, verbose)

    pbar.close()
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
    cache_id: str = "uuid",  # 用于唯一标识处理结果的键，用于缓存
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
        cache_id (str): 用于唯一标识处理结果的键，用于缓存

    Returns:
        list[dict]: 处理后的结果列表，包含原始数据和处理结果

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
            cache_id=cache_id,
        )
    )

if __name__ == "__main__":
    jsonlist = [{"id": i, "text": "Hello World"} for i in range(1000)]
    def work_func(item):
        item["text"] = item["text"].upper()
        return item
    results = xmap(jsonlist, work_func, output_path="output.jsonl", batch_size=2)
    print(results)