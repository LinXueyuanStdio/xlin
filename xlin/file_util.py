from typing_extensions import *
from pathlib import Path
import os
import shutil

from loguru import logger

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
        rm(filepath, filter, expand_all_subdir, debug)
    if dir_path.exists() and dir_path.is_dir() and len(os.listdir(dir_path)) == 0:
        if filter(dir_path):
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
    if output_dir_path.exists() and not output_dir_path.is_dir():
        raise Exception(f"output_dir_path exists and is not a directory: {output_dir_path}")
    if not output_dir_path.exists():
        output_dir_path.mkdir(parents=True, exist_ok=True)
        logger.warning(f"创建文件夹 {output_dir_path}")
    if not base_input_dir.exists():
        raise Exception(f"base_input_dir does not exist: {base_input_dir}")
    if not base_input_dir.is_dir():
        raise Exception(f"base_input_dir is not a directory: {base_input_dir}")
    for input_path in input_paths:
        relative_path = input_path.relative_to(base_input_dir)
        output_path = output_dir_path / relative_path
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


