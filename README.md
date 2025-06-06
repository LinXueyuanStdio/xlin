# xlin

Python 工具代码集合，提供了丰富的工具函数，涵盖文件操作、数据处理、多进程处理等多个方面，旨在提高开发效率。

## 安装

```bash
pip install xlin --upgrade
```

## 使用方法

```python
from xlin import *
```

### 文件操作类：`ls`，`rm` 和 `cp`
- `ls`: 列出文件或文件夹下的所有文件。
- `rm`: 删除文件或文件夹。
- `cp`: 复制文件或文件夹。

```python
from xlin import ls, rm, cp

dir_path = "./data"
dir_path = "/mnt/data.json"
dir_path = "./data,/mnt/data.json"
dir_path = ["./data", "/mnt/data.json", "./data,/mnt/data.json"]
def filter_func(path: Path) -> bool:
    return path.name.endswith('.json')

filepaths: list[Path] = ls(dir_path, filter=filter_func)
rm(dir_path)
cp(dir_path, "./backup_data")  # 会根据最大公共前缀保持文件夹结构
```

### 读取类

- `read_as_json_list`：读取 JSON 文件为列表。
- `read_as_dataframe`：读取文件为表格。如果是文件夹，则读取文件夹下的所有文件为表格并拼接。
- `read_as_dataframe_dict`：读取文件为字典，键为表头，值为列数据。
- `load_text`：加载文本文件。
- `load_yaml`：加载 YAML 文件。
- `load_json`：加载 JSON 文件。
- `load_json_list`：加载 JSON 列表文件。


> `read_as_**` 函数支持文件夹或者文件，支持多种文件格式，包括 Excel、CSV、JSON、Parquet 等。
>
> `load_**` 函数主要用于加载单个文件，支持文本、YAML 和 JSON 格式。

```python
from xlin import *
import pandas as pd

dir_path = "./data"
dir_path = "./data,data.xlsx,data.csv,data.json,data.jsonl,data.parquet,data.feather,data.pkl,data.h5,data.txt,data.tsv,data.xml,data.html,data.db"
dir_path = "./data,/mnt/data.json"
dir_path = ["./data", "/mnt/data.json", "./data,/mnt/data.json"]
df_single = read_as_dataframe(dir_path)
jsonlist = read_as_json_list(dir_path)
df_dict = read_as_dataframe_dict(dir_path)  # xlsx or dirs
for sheet_name, df in df_dict.items():
    print(f"Sheet: {sheet_name}")
    print(df)

text = load_text("example.txt")
yaml_data = load_yaml("example.yaml")
json_data = load_json("example.json")
json_list_data = load_json_list("example.jsonl")
```

### 保存类

```python
save_json(data, 'output.json')
save_json_list(jsonlist, 'output.jsonl')
save_df(df, 'output.xlsx')
save_df_dict(df_dict, 'output.xlsx')  # 将 read_as_dataframe_dict 返回的字典保存为 Excel 文件。
save_df_from_jsonlist(jsonlist, 'output_from_jsonlist.xlsx')
append_to_json_list(data, 'output.jsonl')
```

### 并行处理类：`xmap`
高效处理 JSON 列表
1. 支持多进程/多线程。
2. 支持批量处理。批次内可以不被最慢的样本卡住。
3. 支持异步处理。
4. 支持实时缓存。
5. 支持保序输出。

|方法名称|耗时(秒)|加速比|
|----|----|----|
|普通for循环|150.5713|-|
|xmap(非批量)|38.2211|3.94x|
|xmap(批量)|41.3710|3.64x|
|异步xmap(非批量)|39.8200|3.78x|
|异步xmap(批量)|12.3701|12.17x|

```python
from xlin import xmap, xmap_async

jsonlist = [{"id": i, "value": "Hello World"} for i in range(100)]

def fast_work_func(item):
    item["value"] = item["value"].upper()
    return item

def slow_work_func(item):
    item = fast_work_func(item)
    process_time = random.uniform(1, 2)
    time.sleep(process_time)  # 模拟处理延迟
    return item

def batch_work_func(items):
    return [slow_work_func(item) for item in items]

async def async_work_func(item):
    item = fast_work_func(item)
    await asyncio.sleep(random.uniform(1, 2))
    return item

async def async_batch_work_func(items):
    return await asyncio.gather(*(async_work_func(item) for item in items))

# 在一般的函数中使用
results = xmap(jsonlist, slow_work_func)
results = xmap(jsonlist, batch_work_func, is_batch_work_func=True)
results = xmap(jsonlist, async_work_func, is_async_work_func=True)
results = xmap(jsonlist, async_batch_work_func, is_async_work_func=True, is_batch_work_func=True)

# 在 async 函数中使用
results = await xmap_async(jsonlist, slow_work_func)
results = await xmap_async(jsonlist, batch_work_func, is_batch_work_func=True)
results = await xmap_async(jsonlist, async_work_func, is_async_work_func=True)
results = await xmap_async(jsonlist, async_batch_work_func, is_async_work_func=True, is_batch_work_func=True)
```

网络请求示例：
```python
from xlin import xmap
import aiohttp

async def fetch_url(item: dict):
    async with aiohttp.ClientSession() as session:
        async with session.get(item['url']) as response:
            return {'url': item['url'], 'data': await response.json()}

urls_data = [
    {"url": "https://www.example.com"},
    {"url": "https://www.example.org"},
    {"url": "https://www.example.net"},
]
result = xmap(urls_data, fetch_url, is_async_work_func=True)
```

函数参数说明
```python
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
```

### 合并多个文件：`merge_json_list`，`merge_multiple_df_dict`
合并多个 JSONL 文件。

```python
from xlin import merge_json_list

filenames = ['example1.jsonl', 'example2.jsonl']
output_filename = 'merged.jsonl'
merge_json_list(filenames, output_filename)
```

合并多个 `read_as_dataframe_dict` 返回的字典。

```python
from xlin import read_as_dataframe_dict, merge_multiple_df_dict

df_dict1 = read_as_dataframe_dict('example1.xlsx')
df_dict2 = read_as_dataframe_dict('example2.xlsx')
merged_df_dict = merge_multiple_df_dict([df_dict1, df_dict2])
for sheet_name, df in merged_df_dict.items():
    print(f"Sheet: {sheet_name}")
    print(df)
```

### 对 json 文件批量操作
- 对 JSON 列表应用更改：`apply_changes_to_paths`，`apply_changes_to_jsonlist`

```python
from xlin import *

paths = [Path('example1.jsonl'), Path('example2.jsonl')]
jsonlist = [{"id": 1, "text": "Hello"}, {"id": 2, "text": "World"}]

def change_func(row):
    if row["id"] == 1:
        row["text"] = "New Hello"
        return "updated", row
    return "unchanged", row

changes = {"update_text": change_func}

# 1. 对文件路径应用更改
apply_changes_to_paths(paths, changes, save=True)
# 2. 对 JSON 列表应用更改
new_jsonlist, updated, deleted = apply_changes_to_jsonlist(jsonlist, changes)
print(new_jsonlist)
```

### 生成器
- 从多个文件中生成 JSON 列表的生成器：`generator_from_paths`

```python
from xlin import generator_from_paths
from pathlib import Path

paths = [Path('example1.jsonl'), Path('example2.jsonl')]

for path, row in generator_from_paths(paths):
    print(f"Path: {path}, Row: {row}")
```

### 数据转换
- DataFrame 和 JSON 列表之间的转换：`dataframe_to_json_list` 和 `jsonlist_to_dataframe`

```python
from xlin import dataframe_to_json_list, jsonlist_to_dataframe
import pandas as pd

data = {'col1': [1, 2], 'col2': [3, 4]}
df = pd.DataFrame(data)

json_list = dataframe_to_json_list(df)
print(json_list)

new_df = jsonlist_to_dataframe(json_list)
print(new_df)
```

### 分组
- 对 DataFrame 进行分组：`grouped_col_list`、`grouped_col` 和 `grouped_row`

```python
from xlin import grouped_col_list, grouped_col, grouped_row
import pandas as pd

data = {'query': ['a', 'a', 'b'], 'output': [1, 2, 3]}
df = pd.DataFrame(data)

grouped_col_list_result = grouped_col_list(df)
print(grouped_col_list_result)

grouped_col_result = grouped_col(df)
print(grouped_col_result)

grouped_row_result = grouped_row(df)
print(grouped_row_result)
```

- 对 JSON 列表进行分组：`grouped_row_in_jsonlist`

```python
from xlin import grouped_row_in_jsonlist

jsonlist = [{"query": "a", "output": 1}, {"query": "a", "output": 2}, {"query": "b", "output": 3}]
grouped_row_in_jsonlist_result = grouped_row_in_jsonlist(jsonlist)
print(grouped_row_in_jsonlist_result)
```

### 工具类

- `random_timestamp` 和 `random_timestamp_str`：生成随机时间戳和格式化的随机时间字符串。

```python
from xlin import random_timestamp, random_timestamp_str

timestamp = random_timestamp()
print(timestamp)

timestamp_str = random_timestamp_str()
print(timestamp_str)
```


- `df_dict_summary`: 对 `read_as_dataframe_dict` 返回的字典进行总结，返回一个 DataFrame 包含每个表的基本信息。

```python
from xlin import read_as_dataframe_dict, df_dict_summary

df_dict = read_as_dataframe_dict('example.xlsx')
summary = df_dict_summary(df_dict)
print(summary)
```

- `text_is_all_chinese` 和 `text_contains_chinese`：判断文本是否全为中文或是否包含中文。

```python
from xlin import text_is_all_chinese, text_contains_chinese

text1 = "你好"
text2 = "Hello 你好"

print(text_is_all_chinese(text1))  # True
print(text_is_all_chinese(text2))  # False
print(text_contains_chinese(text2))  # True
```

## 许可证

本项目采用 MIT 许可证，详情请参阅 [LICENSE](LICENSE) 文件。

## 作者

- LinXueyuanStdio <23211526+LinXueyuanStdio@users.noreply.github.com>