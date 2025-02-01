from typing import *
from collections import defaultdict
from pathlib import Path
import pandas as pd
import os
import asyncio
import datetime
from loguru import logger
import shutil
import random


date_str = datetime.datetime.now().strftime("%Y%m%d")
datetime_str = datetime.datetime.now().strftime("%Y%m%d_%Hh%Mm%Ss")


def random_timestamp(start_timestamp=None, end_timestamp=None):
    if start_timestamp is None:
        start_timestamp = datetime.datetime(2024, 1, 1).timestamp()
    if end_timestamp is None:
        end_timestamp = datetime.datetime.now().timestamp()
    return random.uniform(start_timestamp, end_timestamp)


def random_timestamp_str(start_timestamp=None, end_timestamp=None, format="%Y年%m月%d日%H时%M分"):
    return datetime.datetime.fromtimestamp(random_timestamp(start_timestamp, end_timestamp)).strftime(format)


def auto_retry_to_get_data(retry_times, request, data_key="data", *args, **kwargs):
    if retry_times == 0:
        return {}
    resp = request(*args, **kwargs)
    if resp is not None:
        if data_key is None:
            return resp
        elif data_key in resp and resp[data_key] is not None:
            return resp[data_key]
    logger.debug("[error! retrying...]", resp)
    return auto_retry_to_get_data(retry_times - 1, request, data_key, *args, **kwargs)


def append_column(df: pd.DataFrame, query_column: str, output_column: str, transform):
    query = df[query_column].tolist()
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(transform(query))
    df[output_column] = [str(r) for r in result]
    return df


def request_wrapper(request_num=10):
    def request_wrapper_body(func):
        def wrapper(*args, **kwargs):
            c = request_num
            excute_num = 0
            while c > 0:
                c -= 1
                res = func(*args, **kwargs)
                excute_num += 1
                if res != "-1":
                    logger.debug("{} excute_num: {}".format(func.__name__, excute_num))
                    return res
            logger.debug("{} excute_num: {}".format(func.__name__, excute_num))
            return ""

        return wrapper

    return request_wrapper_body


def copy_file(input_filepath, output_filepath, force_overwrite=False, verbose=False):
    if verbose:
        logger.info(f"正在复制 {input_filepath} 到 {output_filepath}")
    if not isinstance(output_filepath, Path):
        output_filepath = Path(output_filepath)
    if output_filepath.exists() and not force_overwrite:
        if verbose:
            logger.warning(f"文件已存在，跳过复制：{output_filepath}")
        return output_filepath
    shutil.copy(input_filepath, output_filepath, follow_symlinks=True)
    return output_filepath


def rm(dir_path: Union[str, Path, List[str], List[Path]], filter: Callable[[Path], bool] = lambda filepath: True, expand_all_subdir=True, debug=False):
    if isinstance(dir_path, str) and "," in dir_path:
        for path in dir_path.split(","):
            rm(path, filter, expand_all_subdir)
        return
    if isinstance(dir_path, list):
        for path in dir_path:
            rm(path, filter, expand_all_subdir)
        return
    dir_path = Path(dir_path)
    if not dir_path.exists():
        if debug:
            print(f"路径不存在 {dir_path}")
        return
    if not dir_path.is_dir():
        if filter(dir_path):
            dir_path.unlink()
            if debug:
                print(f"删除文件 {dir_path}")
        return
    filenames = os.listdir(dir_path)
    for filename in sorted(filenames):
        filepath = dir_path / filename
        if debug:
            print("checking", filepath)
        if filepath.is_dir():
            paths = ls(filepath, filter, expand_all_subdir)
            if len(paths) > 0:
                rm(paths, filter, expand_all_subdir)
            child = filepath
            while child.exists() and len(os.listdir(child)) > 0:
                child = child / os.listdir(child)[0]
            while child != filepath:
                if child.exists() and len(os.listdir(child)) == 0:
                    child.rmdir()
                    if debug:
                        print(f"删除空文件夹 {child}")
                else:
                    break
            if filepath.exists() and len(os.listdir(filepath)) == 0:
                filepath.rmdir()
                if debug:
                    print(f"删除空文件夹 {filepath}")
        elif filter(filepath):
            rm(filepath, filter, expand_all_subdir)
    if dir_path.exists() and len(os.listdir(dir_path)) == 0:
        dir_path.rmdir()
        if debug:
            print(f"删除空文件夹 {dir_path}")


def cp(
    input_dir_path: Union[str, Path, List[str], List[Path]],
    output_dir_path: Union[str, Path],
    base_input_dir: Optional[Union[Path, str]] = None,
    force_overwrite: bool = False,
    filter: Callable[[Path], bool] = lambda filepath: True,
    expand_all_subdir=True,
    verbose=False,
):
    input_paths = ls(input_dir_path, filter, expand_all_subdir)
    if len(input_paths) == 0:
        if verbose:
            logger.warning(f"no files in {input_dir_path}")
        return
    if base_input_dir is None:
        # 计算最大公共路径
        if len(input_paths) > 1:
            base_input_dir = os.path.commonpath([str(p) for p in input_paths])
        else:
            base_input_dir = input_paths[0].parent
    base_input_dir = Path(base_input_dir)
    output_dir_path = Path(output_dir_path)
    for input_path in input_paths:
        relative_path = input_path.relative_to(base_input_dir)
        output_path = output_dir_path / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        copy_file(input_path, output_path, force_overwrite, verbose)


def ls(dir_path: Union[str, Path, List[str], List[Path]], filter: Callable[[Path], bool] = lambda filepath: True, expand_all_subdir=True):
    """list all files, return a list of filepaths

    Args:
        dir_path (Union[str, Path]): dir
        filter ((Path) -> bool, optional): filter. Defaults to lambda filepath:True.
        expand_all_subdir (bool, optional): _description_. Defaults to True.

    Returns:
        List[Path]: not null, may be empty list []
    """
    filepaths: List[Path] = []
    if isinstance(dir_path, str) and "," in dir_path:
        for path in dir_path.split(","):
            filepaths.extend(ls(path, filter, expand_all_subdir))
        return filepaths
    if isinstance(dir_path, list):
        for path in dir_path:
            filepaths.extend(ls(path, filter, expand_all_subdir))
        return filepaths
    dir_path = Path(dir_path)
    if not dir_path.exists():
        return filepaths
    if not dir_path.is_dir():
        if filter(dir_path):
            return [dir_path]
        else:
            return filepaths
    filenames = os.listdir(dir_path)
    for filename in sorted(filenames):
        filepath = dir_path / filename
        if filepath.is_dir():
            if expand_all_subdir:
                filepaths.extend(ls(filepath, filter, expand_all_subdir))
        elif filter(filepath):
            filepaths.append(filepath)
    return filepaths


def clean_empty_folder(dir_path):
    dir_path = Path(dir_path)
    sub_names = os.listdir(dir_path)
    if not sub_names or len(sub_names) == 0:
        print(f"clean empty folder: {dir_path}")
        dir_path.rmdir()
        clean_empty_folder(dir_path.parent)
    else:
        for sub_name in sub_names:
            path = dir_path / sub_name
            if path.is_dir():
                clean_empty_folder(path)


def grouped_col_list(df: pd.DataFrame, key_col="query", value_col="output"):
    grouped = defaultdict(list)
    if key_col not in df.columns:
        logger.warning(f"`{key_col}` not in columns: {list(df.columns)}")
        return grouped
    for i, row in df.iterrows():
        grouped[row[key_col]].append(row[value_col])
    return grouped


def grouped_col(df: pd.DataFrame, key_col="query", value_col="output"):
    grouped = {}
    if key_col not in df.columns:
        logger.warning(f"`{key_col}` not in columns: {list(df.columns)}")
        return grouped
    for i, row in df.iterrows():
        grouped[row[key_col]] = row[value_col]
    return grouped


def grouped_row(df: pd.DataFrame, key_col="query"):
    grouped = defaultdict(list)
    if key_col not in df.columns:
        logger.warning(f"`{key_col}` not in columns: {list(df.columns)}")
        return grouped
    for i, row in df.iterrows():
        grouped[row[key_col]].append(row)
    return grouped


def grouped_row_in_jsonlist(jsonlist: List[Dict[str, Any]], key_col="query"):
    grouped = defaultdict(list)
    for i, row in enumerate(jsonlist):
        if key_col not in row:
            logger.warning(f"`{key_col}` not in row: {row}")
            notfound_key = f"NotFound:{key_col}"
            grouped[notfound_key].append(row)
            continue
        grouped[row[key_col]].append(row)
    return grouped


def submit_file(path: Union[str, Path], target_dir: Union[str, Path]):
    p = Path(path).absolute()
    target_dir = Path(target_dir).absolute()
    logger.info(f"正在复制到目标文件夹 {target_dir}")
    if p.is_dir():
        logger.info(f"文件夹 {p}")
        filenames = os.listdir(path)
        for filename in filenames:
            src_file = p / filename
            tgt_file = target_dir / filename
            copy_file(src_file, tgt_file)
            logger.info(f"已复制 {filename} 到 {tgt_file}")
    else:
        filename = p.name
        logger.info(f"文件 {filename}")
        src_file = p
        tgt_file = target_dir / filename
        copy_file(src_file, tgt_file)
        logger.info(f"已复制 {filename} 到 {tgt_file}")
    filenames = os.listdir(target_dir)
    logger.info("现在目标文件夹下的文件有：\n" + "\n".join(filenames))


def pretty_limited_text(text: str, limited_length: int = 300, language="zh"):
    text = str(text).strip()
    if len(text) > limited_length:
        # if language == "zh":
        #     tail = f"...(共{len(text)}字)"
        # else:
        #     tail = f"...({len(text)} words in total)"
        # return text[: limited_length - len(tail)] + tail
        return text[: limited_length // 2] + text[-limited_length // 2 :]
    return text


def bucket_count(length):
    grouped_count = []
    j = 0
    for i in range(0, max(length), 50):
        grouped_count.append(0)
        while length[j] < i:
            grouped_count[i // 50] += 1
            j += 1
    for i, j in enumerate(grouped_count):
        if i == 0 or j == 0:
            continue
        print(f"[{(i-1)*50}, {i*50})  {j}   {sum(grouped_count[:i+1])/len(length)*100:.2f}%")


def sortedCounter(obj, by="key", reverse=False, return_list=False):
    c = Counter(obj)
    c_list = [(k, c[k]) for k in c]
    if by == "key":
        c_list = sorted(c_list, key=lambda x: x[0], reverse=reverse)
    elif by in ["value", "count"]:
        c_list = sorted(c_list, key=lambda x: x[1], reverse=reverse)
    else:
        raise Exception(f"unsupported by: {by}")
    c = Counter()
    for k, v in c_list:
        c[k] = v
    if return_list:
        return c, c_list
    return c
