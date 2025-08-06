from typing import *
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from loguru import logger
import asyncio
import time
import heapq
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from threading import Lock

# Rich 相关导入
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn
from rich.text import Text
from rich.columns import Columns
from rich.rule import Rule
from rich.console import Group

from xlin.jsonlist_util import append_to_json_list, load_json_list


@dataclass
class TaskReporter:
    """任务进度报告器"""
    worker_id: str
    _current_state: str = ""
    _progress: float = 0.0
    _lock: Lock = field(default_factory=Lock)
    _manager: Optional['TaskManager'] = None

    def set_current_state(self, state: str):
        """设置当前状态"""
        with self._lock:
            self._current_state = state
            if self._manager:
                self._manager._update_worker_state(self.worker_id, state, self._progress)

    def set_progress(self, progress: float):
        """设置进度 (0.0-1.0)"""
        with self._lock:
            self._progress = max(0.0, min(1.0, progress))
            if self._manager:
                self._manager._update_worker_state(self.worker_id, self._current_state, self._progress)

    def get_state(self):
        """获取当前状态"""
        with self._lock:
            return self._current_state, self._progress


@dataclass
class WorkerInfo:
    """工作线程信息"""
    worker_id: str
    current_task_id: str = ""
    current_state: str = "空闲"
    progress: float = 0.0
    start_time: Optional[datetime] = None
    completed_tasks: int = 0
    current_batch_size: int = 0  # 当前正在处理的批次大小
    is_batch_processing: bool = False  # 是否在进行批次处理


@dataclass
class TaskInfo:
    """任务信息"""
    task_id: str
    status: str = "等待中"  # 等待中, 处理中, 已完成, 失败
    worker_id: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_msg: str = ""


class TaskManager:
    """任务管理器"""
    def __init__(self, max_workers: int, desc: str = "Processing", output_path: Optional[str] = None):
        self.max_workers = max_workers
        self.desc = desc
        self.output_path = output_path
        self.workers: Dict[str, WorkerInfo] = {}
        self.tasks: Dict[str, TaskInfo] = {}
        self.console = Console()
        self.lock = Lock()

        # 统计信息
        self.total_tasks = 0
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.cached_tasks = 0
        self.start_time = datetime.now()

        # 缓存文件信息
        self.cache_file_size = 0
        self.cache_bytes_written = 0

        # 初始化工作线程信息
        for i in range(max_workers):
            worker_id = f"Worker-{i+1:03d}"
            self.workers[worker_id] = WorkerInfo(worker_id=worker_id)

    def _update_worker_state(self, worker_id: str, state: str, progress: float):
        """更新工作线程状态"""
        with self.lock:
            if worker_id in self.workers:
                worker = self.workers[worker_id]
                worker.current_state = state
                worker.progress = progress

    def assign_task(self, task_id: str, worker_id: str, batch_size: int = 1):
        """分配任务给工作线程"""
        with self.lock:
            if task_id not in self.tasks:
                self.tasks[task_id] = TaskInfo(task_id=task_id)

            task = self.tasks[task_id]
            worker = self.workers[worker_id]

            task.status = "处理中"
            task.worker_id = worker_id
            task.start_time = datetime.now()

            worker.current_task_id = task_id
            worker.current_state = "开始处理"
            worker.progress = 0.0
            worker.start_time = datetime.now()
            worker.current_batch_size = batch_size
            worker.is_batch_processing = batch_size > 1

    def complete_task(self, task_id: str, success: bool = True, error_msg: str = "", completed_items: int = 1):
        """完成任务"""
        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                worker = self.workers.get(task.worker_id)

                task.status = "已完成" if success else "失败"
                task.end_time = datetime.now()
                task.error_msg = error_msg

                if worker:
                    worker.current_task_id = ""
                    worker.current_state = "空闲"
                    worker.progress = 0.0
                    worker.completed_tasks += 1
                    worker.current_batch_size = 0
                    worker.is_batch_processing = False

                if success:
                    self.completed_tasks += completed_items  # 按实际处理的数据项数增加
                else:
                    self.failed_tasks += completed_items

    def update_cache_progress(self, current_file_size: int = None):
        """更新缓存进度 - 根据任务完成进度估算最终文件大小"""
        with self.lock:
            # 获取当前文件大小
            if current_file_size is not None:
                self.cache_bytes_written = current_file_size

            # 计算任务完成进度
            total_processed = self.completed_tasks
            if self.total_tasks > 0 and total_processed > 0:
                task_progress = total_processed / self.total_tasks

                # 如果任务已完成，直接使用当前文件大小
                if task_progress >= 0.999:  # 基本完成
                    self.cache_file_size = max(self.cache_bytes_written, self.cache_file_size)
                elif task_progress > 0.05:  # 有足够的样本进行估算
                    estimated_final_size = self.cache_bytes_written / task_progress
                    # 确保估算值不小于当前文件大小
                    estimated_final_size = max(estimated_final_size, self.cache_bytes_written)

                    # 使用移动平均来平滑估算值，避免剧烈波动
                    if self.cache_file_size == 0:
                        self.cache_file_size = estimated_final_size
                    else:
                        # 如果新估算值更大，给它更高的权重
                        if estimated_final_size > self.cache_file_size:
                            self.cache_file_size = self.cache_file_size * 0.6 + estimated_final_size * 0.4
                        else:
                            self.cache_file_size = self.cache_file_size * 0.8 + estimated_final_size * 0.2
                else:
                    # 如果进度太小，使用保守估算
                    if self.cache_file_size == 0:
                        self.cache_file_size = max(self.cache_bytes_written * 20, 1024)  # 估算最终大小为当前的20倍

    def create_reporter(self, worker_id: str) -> TaskReporter:
        """创建任务报告器"""
        reporter = TaskReporter(worker_id=worker_id, _manager=self)
        return reporter

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.lock:
            elapsed = datetime.now() - self.start_time

            # 计算活跃工作线程数
            active_workers = sum(1 for w in self.workers.values() if w.current_task_id != "")

            # 计算处理速度
            total_processed = self.completed_tasks + self.failed_tasks
            speed = total_processed / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0

            return {
                "total_tasks": self.total_tasks,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
                "cached_tasks": self.cached_tasks,
                "pending_tasks": self.total_tasks - self.completed_tasks - self.failed_tasks,
                "active_workers": active_workers,
                "idle_workers": self.max_workers - active_workers,
                "elapsed_time": elapsed,
                "processing_speed": speed,
                "eta": timedelta(seconds=(self.total_tasks - total_processed) / speed) if speed > 0 else None
            }

    def create_display(self) -> Layout:
        """创建显示布局"""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=5)  # 减小footer高度
        )

        return layout

    def update_display(self, layout: Layout):
        """更新显示内容"""
        stats = self.get_stats()

        # 头部 - 总体进度
        total_progress = (stats["completed_tasks"] + stats["failed_tasks"]) / max(stats["total_tasks"], 1)
        header_text = Text()
        header_text.append(f"🚀 {self.desc} ", style="bold blue")
        header_text.append(f"[{stats['completed_tasks']}/{stats['total_tasks']}] ", style="green")
        header_text.append(f"{total_progress:.1%}", style="yellow")

        if stats["eta"]:
            header_text.append(f" | ETA: {str(stats['eta']).split('.')[0]}", style="cyan")

        layout["header"].update(Panel(header_text, title="Task Manager", border_style="blue"))

        # 主体 - Workers 横向排列 (类似 nvitop)
        worker_panels = []
        with self.lock:
            for worker_id, worker in self.workers.items():
                # 创建固定格式的单行worker信息
                worker_line = Text()

                # Worker ID (固定宽度12字符)
                worker_line.append(f"{worker_id:<12}", style="bold white")
                worker_line.append(" │ ", style="dim white")

                # 任务信息 (固定宽度30字符) + 批次信息
                if worker.current_task_id:
                    task_display = worker.current_task_id[:25] + "..." if len(worker.current_task_id) > 25 else worker.current_task_id

                    # 如果是批次处理，显示批次信息
                    if worker.is_batch_processing:
                        batch_info = f"({worker.current_batch_size} items)"
                        # 调整task_display长度以容纳批次信息
                        max_task_len = 28 - len(batch_info) - 1  # 减去批次信息和空格
                        if len(task_display) > max_task_len:
                            task_display = task_display[:max_task_len-3] + "..."
                        worker_line.append(f"📋 {task_display} {batch_info:<28}"[:32], style="cyan")
                    else:
                        worker_line.append(f"📋 {task_display:<28}", style="cyan")
                else:
                    worker_line.append(f"💤 {'Idle':<28}", style="dim white")

                worker_line.append(" │ ", style="dim white")

                # 状态信息 (固定宽度20字符)
                state_display = worker.current_state[:18] if worker.current_state else "Waiting for tasks"
                if len(state_display) > 18:
                    state_display = state_display[:15] + "..."

                state_style = "yellow" if worker.current_task_id else "dim yellow"
                worker_line.append(f"🔄 {state_display:<18}", style=state_style)
                worker_line.append(" │ ", style="dim white")

                # 进度条 (固定宽度15字符)
                progress_bar = "█" * int(worker.progress * 10) + "░" * (10 - int(worker.progress * 10))
                progress_text = f"[{progress_bar}] {worker.progress:>3.0%}"
                progress_style = "green" if worker.current_task_id else "dim green"
                worker_line.append(f"{progress_text:<15}", style=progress_style)
                worker_line.append(" │ ", style="dim white")

                # 运行时间 (固定宽度10字符)
                if worker.start_time and worker.current_task_id:
                    elapsed = datetime.now() - worker.start_time
                    time_str = str(elapsed).split('.')[0]
                    if len(time_str) > 8:
                        time_str = time_str[-8:]  # 取后8位
                else:
                    time_str = "--:--:--"

                time_style = "magenta" if worker.current_task_id else "dim magenta"
                worker_line.append(f"⏱️{time_str:>8}", style=time_style)
                worker_line.append(" │ ", style="dim white")

                # 完成任务数 (固定宽度8字符)
                worker_line.append(f"✅ {worker.completed_tasks:>3}", style="bright_green")

                # 根据worker状态设置边框颜色
                border_style = "green" if worker.current_task_id else "dim white"

                worker_panels.append(Panel(
                    worker_line,
                    title=f"Worker {worker_id.split('-')[1]}",
                    border_style=border_style,
                    height=3,  # 固定高度
                    padding=(0, 1)
                ))

        # 将所有 worker 面板垂直排列
        layout["body"].update(Group(*worker_panels))

        # 底部 - 统计信息和缓存进度
        footer_content = Text()

        # 第一行：实时统计 - 紧凑格式
        footer_content.append("📊 ", style="bold blue")
        footer_content.append(f"Total: {stats['total_tasks']} | ", style="white")
        footer_content.append(f"✅ {stats['completed_tasks']} | ", style="green")
        if stats['failed_tasks'] > 0:
            footer_content.append(f"❌ {stats['failed_tasks']} | ", style="red")
        if stats['cached_tasks'] > 0:
            footer_content.append(f"📁 {stats['cached_tasks']} | ", style="yellow")

        # 第二行：工作线程状态 + 速度 + 时间
        footer_content.append("👥 ", style="bold blue")
        footer_content.append(f"Workers: {stats['active_workers']}🟢/{stats['idle_workers']}⚪ ", style="white")
        footer_content.append("│ ", style="dim white")
        footer_content.append(f"⚡ {stats['processing_speed']:.2f} items/sec ", style="yellow")
        footer_content.append("│ ", style="dim white")
        footer_content.append(f"⏰ {str(stats['elapsed_time']).split('.')[0]}", style="magenta")

        # 第三行：缓存进度（如果有）
        if self.cache_file_size > 0 and self.cache_bytes_written > 0:
            # 计算缓存进度，确保不超过100%
            cache_progress = min(self.cache_bytes_written / max(self.cache_file_size, self.cache_bytes_written), 1.0)
            cache_bar = "█" * int(cache_progress * 20) + "░" * (20 - int(cache_progress * 20))

            # 格式化文件大小
            def format_size(bytes_size):
                for unit in ['B', 'KB', 'MB', 'GB']:
                    if bytes_size < 1024:
                        return f"{bytes_size:.1f}{unit}"
                    bytes_size /= 1024
                return f"{bytes_size:.1f}TB"

            footer_content.append(f"\n💾 ", style="bold blue")

            # 处理文件路径显示，只显示文件名或缩短的路径
            display_path = self.output_path or 'output.jsonl'
            if display_path:
                # 如果路径太长，只显示文件名
                from pathlib import Path
                path_obj = Path(display_path)
                if len(str(display_path)) > 50:  # 如果路径太长
                    display_path = f".../{path_obj.name}"
                elif "/" in display_path and not display_path.startswith("./"):
                    # 如果是相对路径但不以./开头，显示相对路径
                    display_path = str(display_path)

            footer_content.append(f"{display_path} ", style="cyan")
            footer_content.append(f"[{cache_bar}] {cache_progress:.0%} ", style="green")
            footer_content.append(f"({format_size(self.cache_bytes_written)}/{format_size(self.cache_file_size)})", style="white")

            # 添加估算准确性指示器
            total_processed = self.completed_tasks
            if total_processed > 0 and self.total_tasks > 0:
                task_progress = total_processed / self.total_tasks
                if task_progress < 0.1:
                    footer_content.append(" 📊", style="dim yellow")  # 估算不够准确
                elif task_progress < 0.5:
                    footer_content.append(" 📈", style="yellow")  # 估算中等准确
                else:
                    footer_content.append(" ✓", style="green")  # 估算较准确

        layout["footer"].update(Panel(footer_content, title="System Status", border_style="cyan"))


async def xmap_async(
    jsonlist: list[Any],
    work_func: Union[
      Callable[[Any, TaskReporter], dict],
      Callable[[list[Any], TaskReporter], list[dict]],
      Awaitable[Callable[[Any, TaskReporter], dict]],
      Awaitable[Callable[[list[Any], TaskReporter], list[dict]]],
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
    """高性能异步数据处理函数，支持可视化进度监控和批次处理。

    xmap_async 提供了一个强大的异步数据处理框架，具有以下特性：
    - 🚀 高并发异步处理，支持同步/异步工作函数
    - 📊 实时可视化进度监控，类似 nvitop 的界面风格
    - 📦 支持批次处理和单项处理两种模式
    - 💾 自动缓存处理结果到 JSONL 文件
    - 🔄 支持任务重试和错误处理
    - ⚡ 自适应工作线程管理和负载均衡

    ## work_func 样例

    ### 单项处理函数：
    ```python
    def work_func(item: Any, reporter: TaskReporter) -> Any:
        # 处理单个数据项
        reporter.set_current_state("处理中...")
        reporter.set_progress(0.5)
        # 处理逻辑
        return processed_item
    ```

    ### 批次处理函数：
    ```python
    def batch_work_func(batch: List[Any], reporter: TaskReporter) -> List[Any]:
        # 处理一批数据项
        batch_size = len(batch)
        results = []
        for i, item in enumerate(batch):
            # 处理单个项目
            results.append(processed_item)
            reporter.set_progress((i + 1) / batch_size)
        return results
    ```

    Args:
        jsonlist (List[Any]): 要处理的数据列表，支持任意数据类型

        work_func (Callable): 工作函数，必须接受两个参数：
            - item/batch: 单个数据项或数据批次
            - reporter: TaskReporter 实例，用于上报进度
            支持四种函数类型：
            - 同步单项: (item, reporter) -> Any
            - 同步批次: (List[item], reporter) -> List[Any]
            - 异步单项: async (item, reporter) -> Any
            - 异步批次: async (List[item], reporter) -> List[Any]

        output_path (Optional[Union[str, Path]], optional):
            输出文件路径，支持 JSONL 格式。如果为 None 则不保存文件。
            默认为 None。

        desc (str, optional):
            任务描述，显示在进度条标题中。默认为 "Processing"。

        max_workers (int, optional):
            最大并发工作线程数。对于 I/O 密集型任务建议 8-32，
            对于 CPU 密集型任务建议设为 CPU 核心数。默认为 8。

        use_process_pool (bool, optional):
            是否使用进程池。True 适用于 CPU 密集型任务，
            False 适用于 I/O 密集型任务。默认为 True。

        preserve_order (bool, optional):
            是否保持输出结果的顺序与输入一致。True 会消耗更多内存，
            但保证顺序；False 性能更好但顺序可能变化。默认为 True。

        retry_count (int, optional):
            任务失败时的重试次数。0 表示不重试。默认为 0。

        force_overwrite (bool, optional):
            是否强制覆盖已存在的输出文件。False 会在文件存在时抛出异常。
            默认为 False。

        is_batch_work_func (bool, optional):
            工作函数是否为批次处理函数。True 时会将数据按 batch_size
            分组后传递给工作函数。默认为 False。

        batch_size (int, optional):
            批次处理时每批的数据量。仅在 is_batch_work_func=True 时生效。
            默认为 32。

        is_async_work_func (bool, optional):
            工作函数是否为异步函数。True 时会使用 await 调用工作函数。
            默认为 False。

        verbose (bool, optional):
            是否输出详细的日志信息，包括错误和重试信息。默认为 False。

    Returns:
        List[Any]: 处理后的结果列表。结果顺序取决于 preserve_order 参数。
                   对于批次处理，返回的是展开后的单项结果列表。

    Raises:
        FileExistsError: 当 output_path 文件已存在且 force_overwrite=False 时
        Exception: 工作函数执行过程中的各种异常

    ## 使用示例

    ### 1. 同步单个处理函数
    ```python
    def process_item(item, reporter: TaskReporter):
        # 处理单个项目
        reporter.set_current_state("处理中...")
        result = {"id": item["id"], "value": item["value"] * 2}
        reporter.set_progress(1.0)
        return result

    results = await xmap_async(jsonlist, process_item)
    ```

    ### 2. 同步批量处理函数
    ```python
    def process_batch(items, reporter: TaskReporter):
        # 处理批量项目
        reporter.set_current_state(f"批量处理 {len(items)} 项")
        results = []
        for i, item in enumerate(items):
            results.append({"id": item["id"], "value": item["value"] * 2})
            reporter.set_progress((i + 1) / len(items))
        return results

    results = await xmap_async(jsonlist, process_batch, is_batch_work_func=True)
    ```

    ### 3. 异步单个处理函数
    ```python
    async def async_process_item(item, reporter: TaskReporter):
        # 异步处理单个项目
        reporter.set_current_state("异步处理中...")
        await asyncio.sleep(0.1)  # 模拟异步操作
        result = {"id": item["id"], "value": item["value"] * 2}
        reporter.set_progress(1.0)
        return result

    results = await xmap_async(jsonlist, async_process_item, is_async_work_func=True)
    ```

    ### 4. 异步批量处理函数
    ```python
    async def async_process_batch(items, reporter: TaskReporter):
        # 异步处理批量项目
        reporter.set_current_state(f"异步批量处理 {len(items)} 项")

        async def process_single_item(item):
            await asyncio.sleep(0.01)  # 模拟异步操作
            return {"id": item["id"], "value": item["value"] * 2}

        results = await asyncio.gather(*[process_single_item(item) for item in items])
        reporter.set_progress(1.0)
        return results

    results = await xmap_async(jsonlist, async_process_batch, is_async_work_func=True, is_batch_work_func=True)
    ```

    ### 5. 简单异步处理
    ```python
    import asyncio
    from rich_xmap import xmap_async, TaskReporter

    async def fetch_data(item, reporter: TaskReporter):
        reporter.set_current_state("正在获取数据...")
        # 模拟异步网络请求
        await asyncio.sleep(0.1)
        reporter.set_progress(1.0)
        return {"id": item["id"], "data": "fetched"}

    data = [{"id": f"item_{i}"} for i in range(100)]
    results = await xmap_async(
        data,
        fetch_data,
        desc="获取数据",
        max_workers=10,
        is_async_work_func=True
    )
    ```

    ### 6. 批次处理示例
    ```python
    def process_batch(batch, reporter: TaskReporter):
        reporter.set_current_state(f"处理批次({len(batch)}项)")
        results = []
        for i, item in enumerate(batch):
            # 批量处理逻辑
            results.append({"processed": item["value"] * 2})
            reporter.set_progress((i + 1) / len(batch))
        return results

    data = [{"value": i} for i in range(1000)]
    results = await xmap_async(
        data,
        process_batch,
        desc="批次处理",
        is_batch_work_func=True,
        batch_size=50,
        output_path="results.jsonl"
    )
    ```

    ### 7. CPU 密集型任务
    ```python
    def cpu_intensive_task(item, reporter: TaskReporter):
        reporter.set_current_state("计算中...")
        # CPU 密集型计算
        result = complex_calculation(item)
        reporter.set_progress(1.0)
        return result

    results = await xmap_async(
        data,
        cpu_intensive_task,
        desc="CPU计算",
        use_process_pool=True,  # 使用进程池
        max_workers=4  # CPU 核心数
    )
    ```

    ## 进度监控界面

    函数运行时会显示类似 nvitop 的实时监控界面：

    ```
    ╭─────────────────────── Task Manager ──────────────────────╮
    │ 🚀 数据处理 [150/200] 75.0% | ETA: 0:00:30              │
    ╰───────────────────────────────────────────────────────────╯
    ╭─────────────── Worker 01 ──────────────╮
    │ Worker-01 │ 📋 Task-0023... │ 🔄 处理中... │ [████████░░] 80% │ ⏱️ 0:00:15 │ ✅ 45 │
    ╰────────────────────────────────────────────────────────╯
    ╭─────────────── Worker 02 ──────────────╮
    │ Worker-02 │ 💤 Idle        │ 🔄 等待任务... │ [░░░░░░░░░░] 0%  │ ⏱️ --:--:-- │ ✅ 38 │
    ╰────────────────────────────────────────────────────────╯
    ╭──────────────────── System Status ────────────────────╮
    │ 📊 Total: 200 | ✅ 150 | 📁 150 | 👥 Workers: 1🟢/7⚪ │ ⚡ 12.5/s │ ⏰ 0:02:15 │
    │ 💾 results.jsonl [██████████████░░░░░░] 75% (1.2MB/1.6MB)                      │
    ╰─────────────────────────────────────────────────────────╯
    ```

    ## 性能建议

    - **I/O 密集型任务**：设置 use_process_pool=False，max_workers=8-32
    - **CPU 密集型任务**：设置 use_process_pool=True，max_workers=CPU核心数
    - **大数据量**：使用批次处理，batch_size=32-128
    - **实时性要求高**：设置 preserve_order=False
    - **网络请求**：设置合适的 retry_count，通常 2-3 次
    """
    jsonlist = list(jsonlist)
    # 初始化任务管理器
    task_manager = TaskManager(max_workers, desc)
    task_manager.total_tasks = len(jsonlist)

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
                if start_idx >= len(jsonlist):
                    return output_list
                task_manager.cached_tasks = start_idx
                if verbose:
                    logger.info(f"继续处理: 已有{start_idx}条记录，共{len(jsonlist)}条")
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)

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

    async def working_at_task(index: int, item: Union[Any, list[Any]], worker_id: str):
        # 创建任务报告器
        reporter = task_manager.create_reporter(worker_id)
        task_id = f"Task-{index:04d}"

        # 确定批次大小
        batch_size_actual = len(item) if is_batch_work_func and isinstance(item, list) else 1

        # 分配任务 - 传递批次大小
        task_manager.assign_task(task_id, worker_id, batch_size_actual)

        try:
            if is_async_work_func:
                result = await work_func(item, reporter)
            else:
                result = await loop.run_in_executor(executor, work_func, item, reporter)

            # 完成任务 - 传递实际处理的项目数
            task_manager.complete_task(task_id, success=True, completed_items=batch_size_actual)
            return index, result
        except Exception as e:
            # 失败任务 - 传递实际处理的项目数
            task_manager.complete_task(task_id, success=False, error_msg=str(e), completed_items=batch_size_actual)
            raise

    # 异步调度
    results = []
    pq = []

    sem = asyncio.Semaphore(max_workers)
    result_queue = asyncio.Queue()

    # 创建可视化界面
    layout = task_manager.create_display()

    async def task_fn(index: int, item: Any | list[Any]):
        worker_id = f"Worker-{(index % max_workers) + 1:03d}"

        # 实现重试逻辑
        for retry_step_idx in range(retry_count + 1):
            async with sem:
                try:
                    result = await working_at_task(index, item, worker_id)
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
                            fallback_result = [{"index": idx, "error": str(e)} for idx in range(index, index + batch_size)]
                        # 将错误结果放入队列
                        await result_queue.put((index, fallback_result))
                        break

    async def producer():
        tasks = []
        for i, item in enumerate(remaining):
            index = i + start_idx
            task = asyncio.create_task(task_fn(index, item))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

    async def consumer():
        next_expect = start_idx
        nonlocal results

        while len(results) < len(jsonlist) - start_idx:
            try:
                idx, res = await asyncio.wait_for(result_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.1)  # 等待结果
                continue  # 继续等待结果
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
                    next_expect += 1  # 批次模式下也是按索引递增

                results.extend(output_buffer)
                if need_caching and output_buffer:
                    append_to_json_list(output_buffer, output_path)
                    task_manager.cached_tasks += len(output_buffer)
                    # 更新缓存进度
                    if output_path and Path(output_path).exists():
                        file_size = Path(output_path).stat().st_size
                        task_manager.update_cache_progress(file_size)
            else:
                # 非保序输出
                if is_batch_work_func:
                    results.extend(res)
                    if need_caching:
                        append_to_json_list(res, output_path)
                        task_manager.cached_tasks += len(res)
                        # 更新缓存进度
                        if output_path and Path(output_path).exists():
                            file_size = Path(output_path).stat().st_size
                            task_manager.update_cache_progress(file_size)
                else:
                    results.append(res)
                    if need_caching:
                        append_to_json_list([res], output_path)
                        task_manager.cached_tasks += 1
                        # 更新缓存进度
                        if output_path and Path(output_path).exists():
                            file_size = Path(output_path).stat().st_size
                            task_manager.update_cache_progress(file_size)


    # 启动生产者和消费者
    with Live(layout, console=task_manager.console, refresh_per_second=4) as live:
        async def update_display():
            # 修正更新条件：使用completed_tasks + failed_tasks来判断是否完成
            while True:
                stats = task_manager.get_stats()
                if stats["completed_tasks"] + stats["failed_tasks"] >= len(jsonlist):
                    break

                task_manager.update_display(layout)
                live.update(layout)
                await asyncio.sleep(0.25)

        # 并行运行生产者、消费者和显示更新
        await asyncio.gather(
            producer(),
            consumer(),
            update_display(),
        )

        # 最后更新一次显示
        task_manager.update_display(layout)
        live.update(layout)

    if not is_async_work_func:
        executor.shutdown(wait=True)

    return jsonlist[:start_idx] + results


import random

def fast_work_func(item, reporter: TaskReporter):
    reporter.set_current_state("转换为大写")
    reporter.set_progress(0.0)

    time.sleep(0.1)  # 模拟工作
    reporter.set_progress(0.5)

    item["value"] = item["value"].upper()

    reporter.set_progress(1.0)
    reporter.set_current_state("完成")
    return item

def slow_work_func(item, reporter: TaskReporter):
    reporter.set_current_state(f"开始处理 {item['id']}")
    reporter.set_progress(0.0)

    # 模拟多步骤处理
    steps = ["预处理", "数据转换", "验证", "后处理", "完成"]
    delay = random.randint(2, 8)

    for i, step in enumerate(steps):
        reporter.set_current_state(step)
        reporter.set_progress(i / len(steps))
        time.sleep(delay / len(steps))

    item = fast_work_func(item, reporter)
    reporter.set_current_state("完成")
    reporter.set_progress(1.0)

    return item

def batch_work_func(items, reporter: TaskReporter):
    reporter.set_current_state("批量处理开始")
    reporter.set_progress(0.0)

    results = []
    total = len(items)

    for i, item in enumerate(items):
        reporter.set_current_state(f"处理项目 {i+1}/{total}")
        reporter.set_progress(i / total)

        # 为每个项目创建临时reporter（这里简化处理）
        temp_reporter = TaskReporter(worker_id=reporter.worker_id)
        result = slow_work_func(item, temp_reporter)
        results.append(result)

    reporter.set_progress(1.0)
    reporter.set_current_state("批量处理完成")
    return results

async def async_work_func(item, reporter: TaskReporter):
    reporter.set_current_state(f"异步开始处理 {item['id']}")
    reporter.set_progress(0.0)

    # 模拟异步多步骤处理
    steps = ["异步预处理", "异步数据转换", "异步验证", "异步后处理"]
    delay = random.randint(1, 5)

    for i, step in enumerate(steps):
        reporter.set_current_state(step)
        reporter.set_progress(i / len(steps))
        await asyncio.sleep(delay / len(steps))

    item["value"] = item["value"].upper()

    reporter.set_progress(1.0)
    reporter.set_current_state("异步处理完成")
    return item

async def async_batch_work_func(items, reporter: TaskReporter):
    reporter.set_current_state("异步批量处理开始")
    reporter.set_progress(0.0)

    # 并行处理批量项目
    async def process_single(item, idx):
        temp_reporter = TaskReporter(worker_id=f"{reporter.worker_id}-{idx}")
        return await async_work_func(item, temp_reporter)

    tasks = [process_single(item, i) for i, item in enumerate(items)]

    # 监控进度
    completed = 0
    results = []
    for task in asyncio.as_completed(tasks):
        result = await task
        results.append(result)
        completed += 1
        reporter.set_progress(completed / len(tasks))
        reporter.set_current_state(f"异步批量处理 {completed}/{len(tasks)}")

    reporter.set_progress(1.0)
    reporter.set_current_state("异步批量处理完成")
    return results


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
    from tqdm import tqdm
    skip_for = True
    skip_batch_async = True
    skip_single_async = False
    skip_batch_sync = True
    skip_single_sync = True
    skip_ordered = True

    # 准备测试数据
    jsonlist = [{"id": i, "value": "Hello World"} for i in range(100)]

    # 临时输出路径
    output_path = Path("test_output.jsonl")
    max_workers = 16
    batch_size = 4

    if skip_for:
        for_time = 10
        for_result = [fast_work_func(item) for item in jsonlist]
    else:
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
    if skip_single_sync:
        xmap_time = 10  # 模拟耗时
        xmap_result = [fast_work_func(item.copy()) for item in jsonlist]
        print(f"跳过xmap函数 (非批量模式) 测试，使用模拟耗时: {xmap_time:.4f}秒")
    else:
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
    if skip_batch_sync:
        xmap_batch_time = 8  # 模拟耗时
        xmap_batch_result = [fast_work_func(item.copy()) for item in jsonlist]
        print(f"跳过xmap函数 (批量模式) 测试，使用模拟耗时: {xmap_batch_time:.4f}秒")
    else:
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

    if not skip_ordered:
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
        if not skip_single_sync and not skip_batch_sync:
            for i, (for_item, xmap_item, xmap_batch_item) in enumerate(zip(for_result, xmap_result, xmap_batch_result)):
                assert for_item["id"] == xmap_item["id"], f"ID不匹配: for循环 {for_item['id']} vs xmap {xmap_item['id']}"
                assert for_item["id"] == xmap_batch_item["id"], f"ID不匹配: for循环 {for_item['id']} vs xmap批量 {xmap_batch_item['id']}"
                assert for_item["value"] == xmap_item["value"], f"值不匹配: for循环 {for_item['value']} vs xmap {xmap_item['value']}"
                assert for_item["value"] == xmap_batch_item["value"], f"值不匹配: for循环 {for_item['value']} vs xmap批量 {xmap_batch_item['value']}"
        else:
            print("跳过同步测试结果验证（因为跳过了某些同步测试）")

    print("\n测试 async xmap 函数性能对比...")
    # 测试异步xmap函数 - 非批量模式
    if skip_single_async:
        async_xmap_time = 6  # 模拟耗时
        async_xmap_result = [fast_work_func(item.copy()) for item in jsonlist]
        print(f"跳过异步xmap函数 (非批量模式) 测试，使用模拟耗时: {async_xmap_time:.4f}秒")
    else:
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
    if skip_batch_async:
        async_xmap_batch_time = 4  # 模拟耗时
        async_xmap_batch_result = [fast_work_func(item.copy()) for item in jsonlist]
        print(f"跳过异步xmap函数 (批量模式) 测试，使用模拟耗时: {async_xmap_batch_time:.4f}秒")
    else:
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

    # 显示for循环结果
    if skip_for:
        print(f"{'普通for循环':<20} {for_time:.4f} (跳过)")
    else:
        print(f"{'普通for循环':<20} {for_time:.4f}")

    # 显示xmap单个模式结果
    if skip_single_sync:
        print(f"{'xmap(非批量)':<20} {xmap_time:.4f} (跳过) {for_time/xmap_time:.2f}x")
    else:
        print(f"{'xmap(非批量)':<20} {xmap_time:.4f} {for_time/xmap_time:.2f}x")

    # 显示xmap批量模式结果
    if skip_batch_sync:
        print(f"{'xmap(批量)':<20} {xmap_batch_time:.4f} (跳过) {for_time/xmap_batch_time:.2f}x")
    else:
        print(f"{'xmap(批量)':<20} {xmap_batch_time:.4f} {for_time/xmap_batch_time:.2f}x")

    if not skip_ordered:
        print(f"{'xmap(保序测试)':<20} {ordered_time:.4f}")

    # 显示异步xmap单个模式结果
    if skip_single_async:
        print(f"{'异步xmap(非批量)':<20} {async_xmap_time:.4f} (跳过) {for_time/async_xmap_time:.2f}x")
    else:
        print(f"{'异步xmap(非批量)':<20} {async_xmap_time:.4f} {for_time/async_xmap_time:.2f}x")

    # 显示异步xmap批量模式结果
    if skip_batch_async:
        print(f"{'异步xmap(批量)':<20} {async_xmap_batch_time:.4f} (跳过) {for_time/async_xmap_batch_time:.2f}x")
    else:
        print(f"{'异步xmap(批量)':<20} {async_xmap_batch_time:.4f} {for_time/async_xmap_batch_time:.2f}x")


if __name__ == "__main__":
  asyncio.run(test_xmap_benchmark())

# 示例用法
async def main():
    # 创建测试数据
    test_data = [
        {"id": f"item-{i:03d}", "value": f"test-value-{i}"}
        for i in range(50)
    ]

    print("🚀 开始异步处理任务演示...")

    # 使用异步工作函数
    results = await xmap_async(
        test_data,
        async_work_func,
        output_path=Path(__file__).parent.parent / "output/async_results.jsonl",
        desc="异步任务处理演示",
        max_workers=8,
        is_async_work_func=True,
        use_process_pool=False,  # 异步函数使用线程池
        verbose=True,
    )

    print(f"\n✅ 处理完成！共处理 {len(results)} 个项目")

if __name__ == "__main__":
    asyncio.run(main())

