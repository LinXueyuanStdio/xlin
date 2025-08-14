import asyncio
from collections import defaultdict
import multiprocessing
from multiprocessing.pool import ThreadPool
import os
import time
from tqdm import tqdm
from typing_extensions import Callable, Dict, List, Optional, Union, Tuple
from pathlib import Path
from loguru import logger

import pandas as pd
import pyexcel

from xlin.file_util import ls
from xlin.jsonlist_util import append_to_json_list, dataframe_to_json_list, load_json, load_json_list, row_to_json, save_json_list
from xlin.multiprocess_util import element_mapping
from xlin.xlsx_util import is_xslx


def read_as_dataframe(
    filepath: Union[str, Path, list[str], list[Path]],
    sheet_name: Optional[str] = None,
    fill_empty_str_to_na=True,
    filter=lambda x: True,
) -> pd.DataFrame:
    """
    读取文件为表格。如果是文件夹，则读取文件夹下的所有文件为表格并拼接
    """
    if isinstance(filepath, list):
        df_list = []
        for path in filepath:
            try:
                df = read_as_dataframe(path, sheet_name, fill_empty_str_to_na, filter)
                df["数据来源"] = path.name
            except:
                df = pd.DataFrame()
            df_list.append(df)
        df = pd.concat(df_list)
        if fill_empty_str_to_na:
            df.fillna("", inplace=True)
        return df
    filepath = Path(filepath)
    if filepath.is_dir():
        paths = ls(filepath, filter=filter, expand_all_subdir=True)
        return read_as_dataframe(paths, sheet_name, fill_empty_str_to_na, filter)
    filename = filepath.name
    if filename.endswith(".json") or filename.endswith(".jsonl"):
        try:
            json_list = load_json(filepath)
        except:
            json_list = load_json_list(filepath)
        df = pd.DataFrame(json_list)
    elif filename.endswith(".xlsx"):
        if sheet_name is None:
            df = pd.read_excel(filepath)
        else:
            df = pd.read_excel(filepath, sheet_name)
    elif filename.endswith(".xls"):
        if is_xslx(filepath):
            if sheet_name is None:
                df = pd.read_excel(filepath)
            else:
                df = pd.read_excel(filepath, sheet_name)
        else:
            df = pyexcel.get_sheet(file_name=filepath)
    elif filename.endswith(".csv"):
        df = pd.read_csv(filepath)
    elif filename.endswith(".parquet"):
        df = pd.read_parquet(filepath)
    elif filename.endswith(".feather"):
        df = pd.read_feather(filepath)
    elif filename.endswith(".pkl"):
        df = pd.read_pickle(filepath)
    elif filename.endswith(".h5"):
        df = pd.read_hdf(filepath)
    elif filename.endswith(".txt"):
        df = pd.read_csv(filepath, delimiter="\t")
    elif filename.endswith(".tsv"):
        df = pd.read_csv(filepath, delimiter="\t")
    elif filename.endswith(".xml"):
        df = pd.read_xml(filepath)
    elif filename.endswith(".html"):
        df = pd.read_html(filepath)[0]
    elif filename.endswith(".db"):
        if sheet_name is None:
            raise ValueError("读取 .db 文件需要提供 sheet_name 作为表名")
        df = pd.read_sql_table(sheet_name, f"sqlite:///{filepath}")
    else:
        raise ValueError((f"Unsupported filetype {filepath}. filetype not in \n" "[json, jsonl, xlsx, xls, csv, " "parquet, feather, pkl, h5, txt, " "tsv, xml, html, db]"))
    if fill_empty_str_to_na:
        df.fillna("", inplace=True)
    return df


def read_as_dataframe_dict(
    filepath: Union[str, Path],
    fill_empty_str_to_na=True,
    filter=lambda x: True,
):
    filepath = Path(filepath)
    if filepath.is_dir():
        paths = ls(filepath, filter=filter, expand_all_subdir=True)
        df_dict_list = []
        for path in paths:
            try:
                df_dict = read_as_dataframe_dict(path, fill_empty_str_to_na, filter)
            except:
                df_dict = {}
            df_dict_list.append(df_dict)
        df_dict = merge_multiple_df_dict(df_dict_list)
        return df_dict
    df_dict: Dict[str, pd.DataFrame] = pd.read_excel(filepath, sheet_name=None)
    if isinstance(df_dict, dict):
        for name, df in df_dict.items():
            if fill_empty_str_to_na:
                df.fillna("", inplace=True)
            df["数据来源"] = filepath.name
    elif isinstance(df_dict, pd.DataFrame):
        if fill_empty_str_to_na:
            df_dict.fillna("", inplace=True)
        df_dict["数据来源"] = filepath.name
    return df_dict


def df_dict_summary(df_dict: Dict[str, pd.DataFrame]):
    rows = []
    for k, df in df_dict.items():
        row = {
            "sheet_name": k,
            "length": len(df),
            "columns": str(list(df.columns)),
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    return df


def save_df_dict(df_dict: Dict[str, pd.DataFrame], output_filepath: Union[str, Path]):
    if not isinstance(output_filepath, Path):
        output_filepath = Path(output_filepath)
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_filepath, engine="xlsxwriter") as writer:
        for k, df in df_dict.items():
            if len(k) > 31:
                logger.warning(f"表名太长，自动截断了：[{k}]的长度为{len(k)}")
            df.to_excel(writer, sheet_name=k[:31], index=False)
    return output_filepath


def save_df_from_jsonlist(
    jsonlist: List[Dict[str, str]],
    output_filepath: Union[str, Path],
):
    df = pd.DataFrame(jsonlist)
    return save_df(df, output_filepath)


def save_df(df: pd.DataFrame, output_filepath: Union[str, Path]):
    if not isinstance(output_filepath, Path):
        output_filepath = Path(output_filepath)
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_filepath, index=False)
    return output_filepath


def lazy_build_dataframe(
    name: str,
    output_filepath: Path,
    func,
    filetype: str = "xlsx",
):
    logger.info(name)
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    if output_filepath.exists():
        df = read_as_dataframe(output_filepath)
    else:
        df: pd.DataFrame = func()
        filename = output_filepath.name.split(".")[0]
        if filetype == "xlsx":
            df.to_excel(output_filepath.parent / f"{filename}.xlsx", index=False)
        elif filetype == "json":
            save_json_list(dataframe_to_json_list(df), output_filepath.parent / f"{filename}.json")
        elif filetype == "jsonl":
            save_json_list(dataframe_to_json_list(df), output_filepath.parent / f"{filename}.jsonl")
        else:
            logger.warning(f"不认识的 {filetype}，默认保存为 xlsx")
            df.to_excel(output_filepath.parent / f"{filename}.xlsx", index=False)
    logger.info(f"{name}结果保存在 {output_filepath}")
    return df


def lazy_build_dataframe_dict(
    name: str,
    output_filepath: Path,
    df_dict: Dict[str, pd.DataFrame],
    func: Callable[[str, pd.DataFrame], pd.DataFrame],
    skip_sheets: List[str] = list(),
):
    logger.info(name)
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    if output_filepath.exists():
        new_df_dict = read_as_dataframe_dict(output_filepath)
    else:
        new_df_dict = {}
        for sheet_name, df in df_dict.items():
            if sheet_name in skip_sheets:
                continue
            df = func(sheet_name, df)
            new_df_dict[sheet_name] = df
        save_df_dict(new_df_dict, output_filepath)
    logger.info(f"{name}结果保存在 {output_filepath}")
    return new_df_dict


def merge_multiple_df_dict(list_of_df_dict: List[Dict[str, pd.DataFrame]], sort=True):
    df_dict_merged = defaultdict(list)
    for df_dict in list_of_df_dict:
        for k, df in df_dict.items():
            df_dict_merged[k].append(df)
    df_dict_merged: Dict[str, pd.DataFrame] = {k: pd.concat(v) for k, v in df_dict_merged.items()}
    if sort:
        df_dict_merged: Dict[str, pd.DataFrame] = {k: df_dict_merged[k] for k in sorted(df_dict_merged)}
    return df_dict_merged


def remove_duplicate_and_sort(df: pd.DataFrame, key_col="query", sort_by="label"):
    query_to_rows = {}
    for i, row in df.iterrows():
        query_to_rows[row[key_col]] = row
    rows = sorted(list(query_to_rows.values()), key=lambda row: row[sort_by])
    df_filtered = pd.DataFrame(rows)
    return df_filtered


def color_negative_red(x):
    color = "red" if x < 0 else ""
    return f"color: {color}"


def highlight_max(x):
    is_max = x == x.max()
    return [("background-color: yellow" if m else "") for m in is_max]


def split_dataframe(
    df: pd.DataFrame,
    output_dir: Union[str, Path],
    output_filename_prefix: str,
    split_count=6,
):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = dataframe_to_json_list(df)
    split_step = len(rows) // split_count + 1
    df_list = []
    for i in range(0, len(rows), split_step):
        filepath = output_dir / f"{output_filename_prefix}_{i // split_step}.xlsx"
        df_i = pd.DataFrame(rows[i : i + split_step])
        df_i.to_excel(filepath, index=False)
        df_list.append(df_i)
    return df_list


def append_column(df: pd.DataFrame, query_column: str, output_column: str, transform):
    query = df[query_column].tolist()
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(transform(query))
    df[output_column] = [str(r) for r in result]
    return df


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


def select_sub_df(
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
    lookback_window: int = 0,
    lookforward_window: int = 0,
    include_end_date: bool = False,
) -> pd.DataFrame:
    """
    从DataFrame中选择指定日期范围内的子DataFrame。

    Args:
        df (pd.DataFrame): 带有日期索引的DataFrame，index是日期。
        start_date (str): 起始日期，格式'YYYY-MM-DD'。
        end_date (str): 结束日期，格式'YYYY-MM-DD'。
        lookback_window (int): 向后查看的天数，默认为0。
        lookforward_window (int): 向前查看的天数，默认为0。
        include_end_date (bool): 是否包含结束日期，默认为False。

    Returns:
        pd.DataFrame: 指定日期范围内的子DataFrame。
    """
    # 确保索引是DatetimeIndex类型
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # 确保索引是有序的
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    # 获取索引的时区信息
    tz = df.index.tz

    # 创建带时区的切片日期
    start = pd.Timestamp(start_date, tz=tz)
    end = pd.Timestamp(end_date, tz=tz)

    # 选择子DataFrame
    try:
        if lookback_window > 0:
            start = start - pd.Timedelta(days=lookback_window)
        if lookforward_window > 0:
            end = end + pd.Timedelta(days=lookforward_window)
        if include_end_date:
            end = end + pd.Timedelta(days=1)
        sub_df = df[start:end]
    except KeyError:
        print(f"日期 {start_date} 或 {end_date} 不在索引范围内。")
        sub_df = pd.DataFrame()

    return sub_df


# region 和 json、jsonlist 的互操作


def dataframe_to_json_list(df: pd.DataFrame):
    """
    Args:
        df (pd.DataFrame): df

    Returns:
        List[Dict[str, str]]: json list: [{"col1": "xxx", "col2": "xxx", ...}, ...]
    """
    json_list = []
    for i, line in df.iterrows():
        json_list.append(dict(line))
    return json_list


def transform_dataframe_to_json_list(df: pd.DataFrame, row_transform):
    """
    Args:
        df (pd.DataFrame): df
        row_transform : lambda row: prompt_template.format(row['query']), "", row['label']

    Returns:
        List[Dict[str, str]]: json list: [{"instruction": "xxx", "input": "xxx", "output": "xxx"}, ...]
    """
    out_list = list()
    for _, row in df.iterrows():
        instruction, input, output = row_transform(row)
        out_list.append({"instruction": instruction, "input": input, "output": output})
    return out_list


def jsonlist_to_dataframe(json_list: List[Dict[str, str]]):
    """
    Args:
        json_list (List[Dict[str, str]]): json list: [{"col1": "xxx", "col2": "xxx", ...}, ...]

    Returns:
        pd.DataFrame: df
    """
    return pd.DataFrame(json_list)


# endregion


# region 并行处理
def dataframe_with_row_mapping(
    df: pd.DataFrame,
    mapping_func: Callable[[dict], Tuple[bool, dict]],
    use_multiprocessing=True,
    thread_pool_size=int(os.getenv("THREAD_POOL_SIZE", 5)),
):
    rows = element_mapping(
        df.iterrows(),
        lambda x: mapping_func(x[1]),
        use_multiprocessing,
        thread_pool_size,
    )
    df = pd.DataFrame(rows)
    return df


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


# endregion
