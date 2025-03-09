from typing import List

import pandas as pd



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
        print(f"[{(i-1)*step}, {i*step})  {j}   {sum(grouped_count[:i+1])/len(length)*100:.2f}%")
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


def draw_histogram(data: list[int], bins=30, title="Data Distribution Analysis"):
    import numpy as np
    import matplotlib.pyplot as plt
    from scipy.stats import gaussian_kde

    data = np.array(data)

    # 计算统计指标
    mean = np.mean(data)
    median = np.median(data)
    std = np.std(data)
    q25, q75 = np.percentile(data, [25, 75])
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
    plt.axvline(mean, color="red", linestyle="--", linewidth=2, label=f"Mean ({mean:.2f})")
    plt.axvline(median, color="green", linestyle="-.", linewidth=2, label=f"Median ({median:.2f})")
    plt.axvspan(mean - std, mean + std, color="orange", alpha=0.1, label=f"±1 Std.Dev ({std:.2f})")

    # 添加四分位线
    plt.axvline(q25, color="purple", linestyle=":", alpha=0.8, label=f"25th Percentile ({q25:.2f})")
    plt.axvline(q75, color="purple", linestyle=":", alpha=0.8, label=f"75th Percentile ({q75:.2f})")

    # 添加统计摘要
    stats_text = f"""\
Data Range: [{data_range[0]:.2f}, {data_range[1]:.2f}]
Observations: {len(data):,}
Standard Deviation: {std:.2f}
IQR: {q75 - q25:.2f}
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
    plt.show()


def draw_pie(numbers: List[int], title="Pie Chart of Numbers"):
    import matplotlib.pyplot as plt

    plt.pie(numbers, labels=[str(i) for i in range(len(numbers))], autopct='%1.1f%%')
    plt.title(title)
    plt.show()