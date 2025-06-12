

from typing_extensions import Literal, Optional, Union
import datetime
import random

import pandas as pd


date_str = datetime.datetime.now().strftime("%Y%m%d")
datetime_str = datetime.datetime.now().strftime("%Y%m%d_%Hh%Mm%Ss")


def random_timestamp(start_timestamp: Optional[float]=None, end_timestamp: Optional[float]=None):
    if start_timestamp is None:
        start_timestamp = datetime.datetime(2024, 1, 1).timestamp()
    if end_timestamp is None:
        end_timestamp = datetime.datetime.now().timestamp()
    return random.uniform(start_timestamp, end_timestamp)


def random_datetime(
    start_datetime: Optional[datetime.datetime] = None,
    end_datetime: Optional[datetime.datetime] = None,
) -> datetime.datetime:
    """
    生成一个随机的 datetime 对象，范围在指定的开始和结束时间之间。
    如果未指定，则默认范围为 2024 年 1 月 1 日到当前时间。
    """
    if start_datetime is None:
        start_datetime = datetime.datetime(2024, 1, 1)
    if end_datetime is None:
        end_datetime = datetime.datetime.now()

    random_timestamp_value = random.uniform(start_datetime.timestamp(), end_datetime.timestamp())
    return datetime.datetime.fromtimestamp(random_timestamp_value)



# 初始化中美节假日（可缓存）懒加载
us_holidays = None # US(categories=US.supported_categories)
cn_holidays = None # CN(categories=CN.supported_categories)


def format_datetime_with_holiday(
    dt: Union[datetime.datetime, str, pd.Series, float],
    language: Literal["zh", "en"] = "zh",
    with_time: bool = True,
    with_weekday: bool = True,
    with_holiday: bool = True,
) -> Union[str, pd.Series]:
    """
    格式化时间为中文日期+英文星期几，附带中美节假日信息。
    如：2024年01月01日 10:00:00 星期一 [假期: 🇨🇳 元旦, 🇺🇸 New Year's Day]
    支持 datetime, str, pandas.Series 批处理。
    Args:
        dt: 待格式化的时间，可以是 datetime, str, pandas.Series 或 timestamp。
        language: 语言选择，支持 "zh" 和 "en"
        with_time: 是否包含时间
        with_weekday: 是否包含星期几
        with_holiday: 是否包含节假日信息
    Returns:
        格式化后的字符串或 pandas.Series
    Raises:
        ValueError: 如果输入类型不正确
        ImportError: 如果未安装 'holidays' 库
    """
    language_dict = {
        "zh": {
            "weekday": ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"],
            "holiday": "假期",
            "date_format": "%Y年%m月%d日",
            "time_format": "%H:%M:%S",
        },
        "en": {
            "weekday": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
            "holiday": "Holiday",
            "date_format": "%Y-%m-%d",
            "time_format": "%H:%M:%S",
        },
    }

    def _format_one(d: Union[datetime.datetime, str]) -> str:
        if isinstance(d, str):
            d = pd.to_datetime(d)
        elif isinstance(d, float):
            d = datetime.datetime.fromtimestamp(d)

        if not isinstance(d, datetime.datetime):
            raise ValueError("输入必须是 datetime, timestamp, str 或 pandas.Series 类型。")

        formatted = d.strftime(language_dict[language]["date_format"])
        if with_time:
            formatted += " " + d.strftime(language_dict[language]["time_format"])
        if with_weekday:
            weekday_index = d.weekday()
            formatted += " " + language_dict[language]["weekday"][weekday_index]
        if not with_holiday:
            return formatted
        # 检查节假日
        global us_holidays, cn_holidays
        if not us_holidays or not cn_holidays:
            try:
                from holidays.countries import US, CN
            except ImportError:
                raise ImportError("请安装 'holidays' 库以支持节假日查询。可以使用 'pip install holidays' 安装。")
            us_holidays = US(categories=US.supported_categories)
            cn_holidays = CN(categories=CN.supported_categories)
        tags = []
        if d in cn_holidays:
            tags.append(f"🇨🇳 {cn_holidays[d]}")
        if d in us_holidays:
            tags.append(f"🇺🇸 {us_holidays[d]}")

        if tags:
            holiday_str = language_dict[language]["holiday"]
            formatted += f" [{holiday_str}: " + ", ".join(tags) + "]"
        return formatted

    if isinstance(dt, pd.Series):
        return dt.apply(_format_one)
    else:
        return _format_one(dt)


def format_timedelta(
    delta: datetime.timedelta,
    language: Literal["zh", "en"] = "zh",
) -> str:
    """
    将 timedelta 格式化为精简的中文可读字符串，省略零值单位，四舍五入到秒

    Args:
        delta: 待格式化的时间间隔
        language: 语言选择，支持 "zh" 和 "en"

    Returns:
        精简的中文时间字符串（如 "1天3小时5分" 或 "45秒"）
    """
    language_dict = {
        "zh": {
            "days": "天",
            "hours": "小时",
            "minutes": "分",
            "seconds": "秒",
        },
        "en": {
            "days": "days",
            "hours": "hours",
            "minutes": "minutes",
            "seconds": "seconds",
        },
    }
    # 处理负数时间（转为正数）
    delta = abs(delta)

    # 分解时间单位（四舍五入到秒）
    days = delta.days
    total_seconds = int(delta.total_seconds() + 0.5)  # 四舍五入到秒
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    # 构建结果列表，跳过零值单位
    parts = []
    if days > 0:
        parts.append(f"{days}{language_dict[language]['days']}")
    if hours > 0:
        parts.append(f"{hours}{language_dict[language]['hours']}")
    if minutes > 0:
        parts.append(f"{minutes}{language_dict[language]['minutes']}")
    if seconds > 0:
        parts.append(f"{seconds}{language_dict[language]['seconds']}")

    # 处理全零情况（如 timedelta(0)）
    return "".join(parts) if parts else f"0{language_dict[language]['seconds']}"
