import sys
from typing_extensions import List, Optional
from collections import Counter, defaultdict

import pandas as pd


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


def bucket_count(length: List[int], step=50, skip_zero_count=False):
    grouped_count = []
    j = 0
    for i in range(0, max(length) + step, step):
        grouped_count.append(0)
        while j < len(length) and length[j] < i:
            grouped_count[i // step] += 1
            j += 1
    x, y = [], []
    for i, j in enumerate(grouped_count):
        if i == 0:
            continue
        if skip_zero_count and j == 0:
            continue
        print(f"[{(i-1)*step}, {i*step})  {j}   {sum(grouped_count[:i+1])/len(length)*100:.4f}%")
        x.append((i - 1) * step)
        y.append(j)
    return x, y


def statistic_char_length(df: pd.DataFrame, instruction_key="instruction"):
    length = []
    for i, row in df.iterrows():
        length.append(len(row[instruction_key]))
    length.sort()
    return length


def statistic_token_length(df: pd.DataFrame, model_path: str, row_to_prompt: lambda row: row["prompt"]):
    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(model_path)
    lengths = []
    for i, row in df.iterrows():
        prompt = row_to_prompt(row)
        inputs = tokenizer(prompt, return_tensors="pt")
        length = inputs["input_ids"].shape[1]
        lengths.append(length)
    lengths.sort()
    return lengths


def draw_histogram(data: list[int], bins=30, title="Data Distribution Analysis", fig_save_path: Optional[str]=None):
    import numpy as np
    import matplotlib.pyplot as plt
    from scipy.stats import gaussian_kde

    data = np.array(data)

    # 计算统计指标
    mean = np.mean(data)
    median = np.median(data)
    std = np.std(data)
    q25, q75, q80, q90 = np.percentile(data, [25, 75, 80, 90])
    data_range = (np.min(data), np.max(data))

    # 创建图形和坐标轴
    plt.figure(figsize=(12, 7), dpi=100)

    # 绘制直方图
    plt.hist(data, bins=bins, density=True, alpha=0.5, color="skyblue", edgecolor="white", label="Distribution")

    # 绘制核密度估计（KDE）
    kde = gaussian_kde(data)
    x_vals = np.linspace(data_range[0] - 1, data_range[1] + 1, 1000)
    plt.plot(x_vals, kde(x_vals), color="navy", linewidth=2, label="KDE Curve")

    # 添加统计线
    plt.axvline(mean, color="red", linestyle="--", linewidth=2, label=f"Mean ({mean:.4f})")
    plt.axvline(median, color="green", linestyle="-.", linewidth=2, label=f"Median ({median:.4f})")
    plt.axvspan(mean - std, mean + std, color="orange", alpha=0.1, label=f"±1 Std.Dev ({std:.4f})")

    # 添加四分位线
    plt.axvline(q25, color="purple", linestyle=":", alpha=0.8, label=f"25th Percentile ({q25:.4f})")
    plt.axvline(q75, color="purple", linestyle=":", alpha=0.8, label=f"75th Percentile ({q75:.4f})")
    plt.axvline(q80, color="purple", linestyle=":", alpha=0.8, label=f"80th Percentile ({q80:.4f})")
    plt.axvline(q90, color="purple", linestyle=":", alpha=0.8, label=f"90th Percentile ({q90:.4f})")

    # 添加统计摘要
    stats_text = f"""\
Data Range: [{data_range[0]:.4f}, {data_range[1]:.4f}]
Observations: {len(data):,}
Standard Deviation: {std:.4f}
IQR: {q75 - q25:.4f}
Skewness: {float((data - mean).mean()**3 / std**3):.4f}
Kurtosis: {float((data - mean).mean()**4 / std**4):.4f}\
"""
# 文字左对齐 align
    plt.annotate(stats_text, xy=(0.99, 0.98), xycoords="axes fraction", ha="right", va="top", fontfamily="monospace", bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),)

    # 设置图形属性
    plt.title(title, fontsize=14, pad=20)
    plt.xlabel("Value", fontsize=12)
    plt.ylabel("Density", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend(loc="upper left", frameon=True, framealpha=0.9, shadow=True)

    # 调整坐标轴范围
    buffer = (data_range[1] - data_range[0]) * 0.1
    plt.xlim(data_range[0] - buffer, data_range[1] + buffer)

    # 显示图形
    plt.tight_layout()
    if fig_save_path is not None:
        plt.savefig(fig_save_path, dpi=300)
    plt.show()

def draw_radar(categories: list[str],
               data: dict[str, list[float]],
               ranges=(0, 5),
               title: str = "Radar Chart",
               size: tuple[float, float] = (6, 6),
               colors: list[str] = None,
               annotation_offset: float = 0.03,
               annotation_kwargs: dict = None,
               chinese_font: bool = False,
               font_name: str = 'Heiti TC',
               font_path: str = None,
               fig_save_path: str = None):
    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties
    """
    Draw a radar chart with optional Chinese font support and configurable annotations.

    Parameters:
    - categories: list of dimension names.
    - data: dict mapping series name -> list of values (same length as categories).
    - ranges: tuple (min, max) for radial axis.
    - title: chart title.
    - size: figure size tuple.
    - colors: list of color hex strings.
    - annotation_offset: fraction of (max-min) to offset text.
    - annotation_kwargs: additional kwargs for plt.text (font size, bbox, etc).
    - chinese_font: whether to enable Chinese font support.
    - font_name: name of a system font to use (overrides defaults).
    - font_path: path to a .ttf font file (used if provided).
    - fig_save_path: if set, saves figure to this path (PNG at 300dpi).
    """
    # Validate data
    N = len(categories)
    for label, values in data.items():
        if len(values) != N:
            raise ValueError(f"Values for '{label}' must have length {N}")

    # Font configuration
    if font_path:
        prop = FontProperties(fname=font_path)
        plt.rcParams['font.family'] = prop.get_name()
    elif font_name:
        plt.rcParams['font.family'] = font_name
    elif chinese_font:
        if sys.platform == "darwin":
            plt.rcParams['font.family'] = ['Heiti TC', 'Arial Unicode MS']
        elif sys.platform.startswith("win"):
            plt.rcParams['font.family'] = ['SimHei', 'Microsoft YaHei']
        else:
            # Fallback to Arial Unicode
            plt.rcParams['font.family'] = ['Arial Unicode MS']
    else:
        plt.rcParams['font.family'] = ['Arial']

    plt.rcParams['axes.unicode_minus'] = False

    # Prepare angles
    angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
    angles += angles[:1]

    # Colors
    default_colors = ["#66c2a5", "#fc8d62", "#8da0cb", "#e78ac3", "#a6d854", "#ffd92f"]
    colors = colors or default_colors

    max_value = max(max(values) for values in data.values())
    min_value = min(min(values) for values in data.values())
    if ranges[0] > min_value or ranges[1] < max_value:
        ranges = (min_value, max_value + 1)

    # Create plot
    fig, ax = plt.subplots(figsize=size, subplot_kw=dict(polar=True))
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, fontsize=12)
    ax.set_ylim(ranges)
    ax.set_rlabel_position(180 / N)
    ax.grid(color='gray', linestyle='--', linewidth=0.5)

    # Plot each series
    for idx, (label, values) in enumerate(data.items()):
        vals = values + values[:1]
        color = colors[idx % len(colors)]
        ax.plot(angles, vals, color=color, linewidth=2, label=label)
        ax.fill(angles, vals, color=color, alpha=0.25)

        # Annotate points
        for angle, val in zip(angles, vals):
            offset = (ranges[1] - ranges[0]) * annotation_offset
            ha = 'left' if np.cos(angle) >= 0 else 'right'
            va = 'bottom' if np.sin(angle) >= 0 else 'top'
            txt_kwargs = dict(fontsize=10, ha=ha, va=va, bbox=dict(boxstyle="round,pad=0.3", edgecolor=color, facecolor='white', alpha=0.8))
            if annotation_kwargs:
                txt_kwargs.update(annotation_kwargs)
            ax.text(angle, val + offset, f"{val}", **txt_kwargs)

    ax.set_title(title, va='bottom', fontsize=16)
    ax.legend(loc='upper right', bbox_to_anchor=(1.15, 1.05))
    plt.tight_layout()

    if fig_save_path:
        plt.savefig(fig_save_path, dpi=300, bbox_inches='tight')
    plt.show()


def draw_preds_labels(preds: list[str], labels: list[str],
                      title="Pred and Label Class Distribution",
                      fig_save_path: Optional[str]=None):
    from collections import Counter
    import matplotlib.pyplot as plt

    out_of_class = "out_of_class"
    valid_values = list(set(labels)) + [out_of_class]
    valid_preds = []
    for pred in preds:
        if pred not in valid_values:
            valid_preds.append(out_of_class)
        else:
            valid_preds.append(pred)

    counter = Counter(valid_preds)
    pred_labels = list(counter.keys())
    pred_values = list(counter.values())

    # 绘制柱状图 pred
    plt.figure(figsize=(12, 12))
    plt.subplot(2, 2, 1)
    plt.bar(pred_labels, pred_values)
    plt.xlabel("class")
    plt.ylabel("count")
    plt.title("pred class distribution")

    # 绘制饼图 pred
    plt.subplot(2, 2, 2)
    plt.pie(pred_values, labels=pred_labels, autopct="%1.1f%%")
    plt.title("pred class distribution")

    # 绘制柱状图 label
    counter = Counter(labels)
    label_labels = list(counter.keys())
    label_values = list(counter.values())
    plt.subplot(2, 2, 3)
    plt.bar(label_labels, label_values)
    plt.xlabel("class")
    plt.ylabel("count")
    plt.title("label class distribution")
    # 绘制饼图 label
    plt.subplot(2, 2, 4)
    plt.pie(label_values, labels=label_labels, autopct="%1.1f%%")
    plt.title("label class distribution")
    plt.suptitle(title)

    plt.tight_layout()
    if fig_save_path is not None:
        plt.savefig(fig_save_path, dpi=300)
    plt.show()


def generate_classification_report(predictions: List[str], labels: List[str]) -> dict:
    """
    生成包含准确率、混淆矩阵、分类报告等详细评估结果的字典

    Args:
        predictions: 模型预测结果列表
        labels: 真实标签列表

    Returns:
        包含以下结构的字典：
        - accuracy: 整体准确率
        - confusion_matrix: 混淆矩阵DataFrame
        - class_report: 分类报告DataFrame
        - error_analysis: 错误样本分析DataFrame
        - total_samples: 总样本数
        - time_generated: 报告生成时间
    """
    # 基础校验
    assert len(predictions) == len(labels), "预测结果与标签长度不一致"

    # 初始化报告字典
    report = {}

    # 获取唯一类别
    classes = sorted(list(set(labels)))
    error_label = "out_of_class"
    extend_classes = classes + [error_label]

    # 计算基础指标
    total = len(labels)
    correct = sum(p == l for p, l in zip(predictions, labels))

    # 1. 准确率计算
    report["accuracy"] = correct / total

    # 2. 混淆矩阵构建
    confusion = defaultdict(int)
    for true_label, pred_label in zip(labels, predictions):
        if pred_label not in classes:
            pred_label = error_label
        confusion[(true_label, pred_label)] += 1

    confusion_matrix = pd.DataFrame(index=classes, columns=extend_classes, data=0)
    for (true, pred), count in confusion.items():
        confusion_matrix.loc[true, pred] = count

    # 3. 分类报告生成
    micro_tp = 0
    micro_fp = 0
    micro_fn = 0
    class_stats = []
    for cls in classes:
        tp = confusion[(cls, cls)]
        fp = sum(confusion[(other, cls)] for other in extend_classes if other != cls)
        fn = sum(confusion[(cls, other)] for other in extend_classes if other != cls)

        if cls != error_label:
            micro_tp += tp
            micro_fp += fp
            micro_fn += fn

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

        class_stats.append(
            {
                "class": cls,
                "precision": precision,
                "recall": recall,
                "f1_score": f1,
                "support": sum(confusion[(cls, other)] for other in extend_classes),
            },
        )

    # 添加汇总统计
    class_df = pd.DataFrame(class_stats)
    report["class_report"] = class_df
    confusion_matrix["recall"] = class_df["recall"].values.tolist()
    p = class_df["precision"].values.tolist() + [-1, -1]  # [out_of_class, recall]
    tail = pd.DataFrame([p], index=["precision"], columns=confusion_matrix.columns)
    confusion_matrix = pd.concat([confusion_matrix, tail], axis=0)
    confusion_matrix.index.name = "True \\ Pred"
    confusion_matrix["sum"] = class_df["support"].values.tolist() + [class_df["support"].sum()]
    report["confusion_matrix"] = confusion_matrix

    micro_precision = micro_tp / (micro_tp + micro_fp) if (micro_tp + micro_fp) > 0 else 0
    micro_recall = micro_tp / (micro_tp + micro_fn) if (micro_tp + micro_fn) > 0 else 0
    micro_f1 = 2 * (micro_precision * micro_recall) / (micro_precision + micro_recall) if (micro_precision + micro_recall) > 0 else 0
    report["micro_stats"] = {
        "precision": micro_precision,
        "recall": micro_recall,
        "f1_score": micro_f1,
    }
    report["macro_stats"] = {
        "precision": class_df[class_df["class"] != error_label]["precision"].mean(),
        "recall": class_df[class_df["class"] != error_label]["recall"].mean(),
        "f1_score": class_df[class_df["class"] != error_label]["f1_score"].mean(),
    }

    # 4. 元数据信息
    import datetime
    report["total_samples"] = total
    report["time_generated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return report


def convert_to_jsonable_report(report_row):
    new_report_json = {}
    for key, value in report_row.items():
        if isinstance(value, dict):
            new_report_json[key] = convert_to_jsonable_report(value)
        elif isinstance(value, list):
            new_report_json[key] = [convert_to_jsonable_report(item) if isinstance(item, dict) else item for item in value]
        elif isinstance(value, pd.DataFrame):
            if value.index.name is not None:
                value = value.reset_index()
            value = value.fillna(-1)
            new_report_json[key] = value.to_dict(orient="records")
        else:
            new_report_json[key] = value
    return new_report_json


def print_classification_report(predictions: List[str], labels: List[str]):
    report = generate_classification_report(predictions, labels)
    """
    打印报告内容
    """
    print(f"准确率: {report['accuracy']:.2%}")
    print(f"总样本数: {report['total_samples']}, 生成时间: {report['time_generated']}")
    print()
    # 打印微观统计
    print("=== 微观统计 ===")
    micro_stats = report["micro_stats"]
    print(f"准确率: {micro_stats['precision']:.2%}")
    print(f"召回率: {micro_stats['recall']:.2%}")
    print(f"F1分数: {micro_stats['f1_score']:.2%}")
    print()
    # 打印宏观统计
    print("=== 宏观统计 ===")
    macro_stats = report["macro_stats"]
    print(f"准确率: {macro_stats['precision']:.2%}")
    print(f"召回率: {macro_stats['recall']:.2%}")
    print(f"F1分数: {macro_stats['f1_score']:.2%}")
    print()

    # 打印混淆矩阵
    print("=== 混淆矩阵 ===")
    print(report["confusion_matrix"])
    print()

    # 打印分类报告
    print("=== 分类报告 ===")
    print(report["class_report"])
    print()
    return report


if __name__ == "__main__":
    # 示例数据
    preds = ["cat", "dog", "cat", "dog", "extra1", "extra2"]
    truth = ["cat", "cat", "dog", "dog", "dog", "dog"]

    print_classification_report(preds, truth)
    draw_preds_labels(preds, truth, title="Pred and Label Class Distribution", fig_save_path="assets/pred_label_distribution.png")

    import random

    lengths = [random.randint(0, 100) for _ in range(100)]
    draw_histogram(lengths, bins=50, title="Length Distribution", fig_save_path="assets/length_distribution.png")

    categories = ['速度', '可靠性', '舒适度', '安全性', '效率', '风格', '性价比']
    data = {
        '产品 A': [4, 3, 2, 5, 4, 3, -1],
        '产品 B': [3, 4, 5, 3, 3, 4, 8]
    }
    draw_radar(categories, data, title="产品对比雷达图", chinese_font=True, fig_save_path="assets/radar_chart.png")
