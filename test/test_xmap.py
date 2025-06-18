import time
from pathlib import Path
from tqdm import tqdm
import asyncio
import time
from pathlib import Path
from tqdm.asyncio import tqdm
from xlin import xmap_async


# 性能测试用例
async def test_xmap_benchmark():
    """
    xmap函数的性能基准测试

    测试内容包括：
    1. 普通for循环 vs xmap性能对比
    2. 单个处理模式 vs 批量处理模式对比
    3. 保序功能正确性验证
    4. 各种配置的性能表现

    测试数据：10000个简单的文本处理任务

    输出：
    - 各种方法的耗时对比
    - 性能加速比
    - 保序功能验证结果
    """
    import random
    # 准备测试数据
    jsonlist = [{"id": i, "value": "Hello World"} for i in range(100)]

    def fast_work_func(item):
        item["value"] = item["value"].upper()
        return item

    def slow_work_func(item):
        print(f"start {item['id']}")
        item = fast_work_func(item)
        time.sleep(random.randint(2, 10))  # 模拟处理延迟
        print(f"end {item['id']}")
        return item

    def batch_work_func(items):
        return [slow_work_func(item) for item in items]

    async def async_work_func(item):
        print(f"start {item['id']}")
        item = fast_work_func(item)
        await asyncio.sleep(random.randint(2, 10))
        print(f"end {item['id']}")
        return item

    async def async_batch_work_func(items):
        return await asyncio.gather(*(async_work_func(item) for item in items))

    # 临时输出路径
    output_path = Path("test_output.jsonl")
    max_workers = 16
    batch_size = 4

    # 测试普通for循环
    print("测试普通for循环...")
    start_time = time.time()
    # 节约时间
    for_result = []
    for item in tqdm(jsonlist):
        # processed = fast_work_func(item)
        processed = slow_work_func(item)
        for_result.append(processed)
    for_time = time.time() - start_time
    # for_result = [{"id": i, "text": "Hello World".upper()} for i in range(100)]
    # for_time = 352.3638
    print(f"普通for循环耗时: {for_time:.4f}秒")

    # 测试xmap函数 - 非批量模式
    print("\n测试xmap函数 (非批量模式)...")
    start_time = time.time()
    xmap_result = await xmap_async(
        jsonlist=jsonlist,
        work_func=slow_work_func,
        output_path=output_path,
        max_workers=max_workers,
        desc="Processing items",
        use_process_pool=False,
        preserve_order=True,
        retry_count=0,
        force_overwrite=True,
        is_batch_work_func=False,
        is_async_work_func=False,
        verbose=False,
    )
    xmap_time = time.time() - start_time
    print(f"xmap函数 (非批量模式) 耗时: {xmap_time:.4f}秒")

    # 测试xmap函数 - 批量模式
    print("\n测试xmap函数 (批量模式)...")
    # 清理之前的输出文件
    output_path = Path("test_output_batch.jsonl")
    start_time = time.time()

    xmap_batch_result = await xmap_async(
        jsonlist=jsonlist,
        work_func=batch_work_func,
        output_path=output_path,
        max_workers=max_workers,
        desc="Processing items in batches",
        use_process_pool=False,
        preserve_order=True,
        retry_count=0,
        force_overwrite=True,
        is_batch_work_func=True,
        batch_size=batch_size,
        is_async_work_func=False,
    )
    xmap_batch_time = time.time() - start_time
    print(f"xmap函数 (批量模式) 耗时: {xmap_batch_time:.4f}秒")

    # 测试保序功能
    print("\n测试xmap函数保序功能...")
    start_time = time.time()

    def slow_work_func(item):
        # 添加随机延迟模拟不同处理时间，延迟与ID成反比，让后面的元素先完成
        import random
        delay = 0.001 * (1000 - item["id"]) / 1000.0  # 后面的ID处理更快
        time.sleep(delay + random.uniform(0, 0.001))
        item["value"] = item["value"].upper()
        item["processed_order"] = item["id"]
        return item

    test_data = jsonlist[:100]  # 使用较少数据进行测试
    xmap_ordered_result = await xmap_async(
        jsonlist=test_data,
        work_func=slow_work_func,
        preserve_order=True,
        max_workers=max_workers,
        use_process_pool=False,
        is_batch_work_func=False,
        verbose=False,
    )
    xmap_unordered_result = await xmap_async(
        jsonlist=test_data,
        work_func=slow_work_func,
        preserve_order=False,
        max_workers=max_workers,
        use_process_pool=False,
        is_batch_work_func=False,
        verbose=False,
    )

    ordered_time = time.time() - start_time
    print(f"保序测试耗时: {ordered_time:.4f}秒")

    # 验证保序结果
    ordered_ids = [item["processed_order"] for item in xmap_ordered_result]
    expected_ids = list(range(100))

    print(f"保序结果正确性: {'✓' if ordered_ids == expected_ids else '✗'}")
    print(f"保序前10个ID: {ordered_ids[:10]}")

    unordered_ids = [item["processed_order"] for item in xmap_unordered_result]
    print(f"非保序前10个ID: {unordered_ids[:10]}")

    # 检查是否有顺序差异
    order_difference = sum(1 for i, (a, b) in enumerate(zip(ordered_ids, unordered_ids)) if a != b)
    print(f"顺序差异数量: {order_difference}/100")
    print(f"保序功能测试: {'✓' if order_difference < len(ordered_ids) else '需要更强的并发测试'}")

    # 验证结果
    for i, (for_item, xmap_item, xmap_batch_item) in enumerate(zip(for_result, xmap_result, xmap_batch_result)):
        assert for_item["id"] == xmap_item["id"], f"ID不匹配: for循环 {for_item['id']} vs xmap {xmap_item['id']}"
        assert for_item["id"] == xmap_batch_item["id"], f"ID不匹配: for循环 {for_item['id']} vs xmap批量 {xmap_batch_item['id']}"
        assert for_item["value"] == xmap_item["value"], f"值不匹配: for循环 {for_item['value']} vs xmap {xmap_item['value']}"
        assert for_item["value"] == xmap_batch_item["value"], f"值不匹配: for循环 {for_item['value']} vs xmap批量 {xmap_batch_item['value']}"

    print("\n测试 async xmap 函数性能对比...")
    # 测试异步xmap函数 - 非批量模式
    print("\n测试异步xmap函数 (非批量模式)...")
    start_time = time.time()
    async_xmap_result = await xmap_async(
        jsonlist=jsonlist,
        work_func=async_work_func,
        output_path=output_path,
        max_workers=max_workers,
        desc="Processing items asynchronously",
        use_process_pool=False,
        preserve_order=True,
        retry_count=0,
        force_overwrite=True,
        is_batch_work_func=False,
        is_async_work_func=True,
        verbose=False,
    )
    async_xmap_time = time.time() - start_time
    print(f"异步xmap函数 (非批量模式) 耗时: {async_xmap_time:.4f}秒")

    # 测试异步xmap函数 - 批量模式
    print("\n测试异步xmap函数 (批量模式)...")
    # 清理之前的输出文件
    output_path = Path("test_output_async_batch.jsonl")
    start_time = time.time()
    async_xmap_batch_result = await xmap_async(
        jsonlist=jsonlist,
        work_func=async_batch_work_func,
        output_path=output_path,
        max_workers=max_workers,
        desc="Processing items in batches asynchronously",
        use_process_pool=False,
        preserve_order=True,
        retry_count=0,
        force_overwrite=True,
        is_batch_work_func=True,
        batch_size=batch_size,
        is_async_work_func=True,
    )
    async_xmap_batch_time = time.time() - start_time
    print(f"异步xmap函数 (批量模式) 耗时: {async_xmap_batch_time:.4f}秒")

    # 输出性能对比
    print("\n===== 性能对比分析 =====")
    print(f"{'方法名称':<20} {'耗时(秒)':<12} {'加速比'}")
    print(f"{'普通for循环':<20} {for_time:.4f}")
    print(f"{'xmap(非批量)':<20} {xmap_time:.4f} {for_time/xmap_time:.2f}x")
    print(f"{'xmap(批量)':<20} {xmap_batch_time:.4f} {for_time/xmap_batch_time:.2f}x")
    print(f"{'xmap(保序测试)':<20} {ordered_time:.4f}")
    print(f"{'异步xmap(非批量)':<20} {async_xmap_time:.4f} {for_time/async_xmap_time:.2f}x")
    print(f"{'异步xmap(批量)':<20} {async_xmap_batch_time:.4f} {for_time/async_xmap_batch_time:.2f}x")


if __name__ == "__main__":
    asyncio.run(test_xmap_benchmark())
