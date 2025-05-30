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
高效处理 JSON 列表，支持多进程/多线程。

```python
from xlin import xmap

jsonlist = [{"id": 1, "text": "Hello"}, {"id": 2, "text": "World"}]

def work_func(item):
    item["text"] = item["text"].upper()
    return item

results = xmap(jsonlist, work_func, output_path="output.jsonl", batch_size=2)
print(results)
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