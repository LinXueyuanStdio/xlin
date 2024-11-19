from collections import defaultdict
import os
from typing import Callable, Dict, List, Optional, Tuple, Union
from pathlib import Path
from loguru import logger

import pandas as pd
import pyexcel

from util import ls
from jsonl import dataframe_to_json_list, load_json, load_json_list, save_json_list
from xls2xlsx import is_xslx


def valid_to_read_as_dataframe(filename: str) -> bool:
    suffix_list = [".json", ".jsonl", ".xlsx", "xls", ".csv"]
    return any([filename.endswith(suffix) for suffix in suffix_list])


def read_as_dataframe(filepath: Union[str, Path], sheet_name: Optional[str] = None, fill_empty_str_to_na=True) -> pd.DataFrame:
    """
    读取文件为表格
    """
    filepath = Path(filepath)
    if filepath.is_dir():
        paths = ls(filepath, expand_all_subdir=True)
        df_list = []
        for path in paths:
            try:
                df = read_as_dataframe(path, sheet_name, fill_empty_str_to_na)
                df['数据来源'] = path.name
            except:
                df = pd.DataFrame()
            df_list.append(df)
        df = pd.concat(df_list)
        if fill_empty_str_to_na:
            df.fillna("", inplace=True)
        return df
    filename = filepath.name
    if filename.endswith(".json") or filename.endswith(".jsonl"):
        try:
            json_list = load_json(filepath)
        except:
            json_list = load_json_list(filepath)
        df = pd.DataFrame(json_list)
    elif filename.endswith(".xlsx"):
        df = pd.read_excel(filepath) if sheet_name is None else pd.read_excel(filepath, sheet_name)
    elif filename.endswith(".xls"):
        if is_xslx(filepath):
            df = pd.read_excel(filepath) if sheet_name is None else pd.read_excel(filepath, sheet_name)
        else:
            df = pyexcel.get_sheet(file_name=filepath)
    elif filename.endswith(".csv"):
        df = pd.read_csv(filepath)
    else:
        raise ValueError(f"Unsupported filetype {filepath}. filetype not in [json, jsonl, xlsx, xls, csv]")
    if fill_empty_str_to_na:
        df.fillna("", inplace=True)
    return df


def read_maybe_dir_as_dataframe(filepath: Union[str, Path], sheet_name: Optional[str] = None) -> pd.DataFrame:
    """
    input path 可能是文件夹，此时将文件夹下的所有表格拼接到一起返回，要求所有表头一致
    如果不是文件夹，则为文件，尝试直接读取为表格返回
    """
    out_list = list()
    if not isinstance(filepath, Path):
        filepath = Path(filepath)
    if not filepath.exists():
        raise ValueError(f"Path Not Exist: {filepath}")
    if not filepath.is_dir():
        return read_as_dataframe(filepath, sheet_name)

    files = os.listdir(filepath)
    for file_name in files:
        if not valid_to_read_as_dataframe(file_name):
            continue
        input_file = filepath / file_name
        df = read_as_dataframe(input_file, sheet_name)
        df.fillna("", inplace=True)
        for _, line in df.iterrows():
            line = line.to_dict()
            out_list.append(line)
    df = pd.DataFrame(out_list)
    return df


def read_as_dataframe_dict(filepath: Union[str, Path], fill_empty_str_to_na=True):
    filepath = Path(filepath)
    if filepath.is_dir():
        paths = ls(filepath, expand_all_subdir=True)
        df_dict_list = []
        for path in paths:
            try:
                df_dict = read_as_dataframe_dict(path, fill_empty_str_to_na)
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
            df['数据来源'] = filepath.name
    elif isinstance(df_dict, pd.DataFrame):
        if fill_empty_str_to_na:
            df_dict.fillna("", inplace=True)
        df_dict['数据来源'] = filepath.name
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


def save_df_from_jsonlist(jsonlist: List[Dict[str, str]], output_filepath: Union[str, Path]):
    df = pd.DataFrame(jsonlist)
    return save_df(df, output_filepath)


def save_df(df: pd.DataFrame, output_filepath: Union[str, Path]):
    if not isinstance(output_filepath, Path):
        output_filepath = Path(output_filepath)
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_filepath, index=False)
    return output_filepath


def lazy_build_dataframe(name: str, output_filepath: Path, func, filetype: str = "xlsx"):
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


def lazy_build_dataframe_dict(name: str, output_filepath: Path, df_dict: Dict[str, pd.DataFrame], func, skip_sheets: List[str] = list()):
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


def remove_duplicate_and_sort(df: pd.DataFrame, key_col="query", sort_by='label'):
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


def split_dataframe(df: pd.DataFrame, output_dir: Union[str, Path], tag: str, split_count=6):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = dataframe_to_json_list(df)
    split_step = len(rows) // split_count + 1
    df_list = []
    for i in range(0, len(rows), split_step):
        filepath = output_dir / f"{tag}_{i // split_step}.xlsx"
        df_i = pd.DataFrame(rows[i:i+split_step])
        df_i.to_excel(filepath, index=False)
        df_list.append(df_i)
    return df_list

