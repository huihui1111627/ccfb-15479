"""可视化: 队列占用随微秒时间变化、动态门限曲线、丢包时刻."""

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = [
    "PingFang SC", "Hiragino Sans GB", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

from .config import PRIO_NAMES, SwitchConfig
from .policies import Policy

PRIO_COLORS = {0: "#d62728", 1: "#1f77b4", 2: "#7f7f7f"}


def plot_dynamics(res, cfg: SwitchConfig, policy: Policy, out_path,
                  victim_port=None, t_range_us=None):
    """三联图: 缓冲占用 / 动态门限 / 丢包时刻."""
    assert res.series is not None, "需要 record_series=True 的仿真结果"
    t = res.series["t"]
    total = res.series["total"]
    prio_occ = res.series["prio_occ"]
    q_occ = res.series["q_occ"]
    P, R = res.series["n_ports"], res.series["n_prio"]
    if victim_port is None:
        victim_port = 0
        if res.incast_events:
            victim_port = res.incast_events[0]["port"]
    if t_range_us is not None:
        m = (t >= t_range_us[0]) & (t <= t_range_us[1])
    else:
        m = np.ones(len(t), dtype=bool)
    tt = t[m]

    fig, axes = plt.subplots(3, 1, figsize=(13, 11), sharex=True)

    # --- 1. 缓冲占用 ---
    ax = axes[0]
    ax.stackplot(tt, *[prio_occ[m, pr] / 1e6 for pr in range(R)],
                 labels=[PRIO_NAMES[pr] for pr in range(R)],
                 colors=[PRIO_COLORS[pr] for pr in range(R)], alpha=0.75)
    ax.plot(tt, total[m] / 1e6, "k-", lw=0.8, label="总占用")
    ax.axhline(cfg.buffer_bytes / 1e6, color="k", ls="--", lw=0.8,
               label=f"总缓冲 {cfg.buffer_bytes/1e6:.0f} MB")
    ax.set_ylabel("缓冲占用 (MB)")
    ax.set_title(f"共享缓冲占用(按优先级堆叠) — 策略 {res.policy_name}")
    ax.legend(loc="upper right", fontsize=8, ncol=2)

    # --- 2. 受害端口各队列占用 vs 动态门限 ---
    ax = axes[1]
    thr_clip = 1.5 * cfg.buffer_bytes   # 门限可能远超 B, 裁剪以便观察
    for pr in range(R):
        ax.plot(tt, q_occ[m, victim_port, pr] / 1e6,
                color=PRIO_COLORS[pr], lw=1.0,
                label=f"端口{victim_port} {PRIO_NAMES[pr]} 队列占用")
        if policy.name == "dt":
            thr = np.array([policy.threshold(pr, tot) for tot in total[m]])
            thr = np.minimum(thr, thr_clip)
            ax.plot(tt, thr / 1e6, color=PRIO_COLORS[pr], ls="--", lw=0.9,
                    label=f"{PRIO_NAMES[pr]} 动态门限")
    ax.set_ylabel("字节 (MB)")
    ax.set_title(f"端口 {victim_port} 队列占用 vs 动态门限(虚线, 超 1.5xB 裁剪)")
    ax.legend(loc="upper right", fontsize=8, ncol=2)

    # --- 3. 丢包时刻 ---
    ax = axes[2]
    drops = res.drops
    if len(drops["t"]):
        in_range = (drops["t"] >= tt[0]) & (drops["t"] <= tt[-1])
        for pr in range(R):
            dm = (drops["prio"] == pr) & in_range
            if dm.any():
                ax.scatter(drops["t"][dm], np.full(dm.sum(), pr),
                           color=PRIO_COLORS[pr], s=6, marker="|",
                           label=f"{PRIO_NAMES[pr]} 丢包 ({dm.sum()})")
    for ev in res.incast_events or []:
        if tt[0] <= ev["t_us"] <= tt[-1]:
            ax.axvline(ev["t_us"], color="purple", ls=":", lw=0.8)
    ax.set_xlim(tt[0], tt[-1])
    ax.set_yticks(range(R))
    ax.set_yticklabels([PRIO_NAMES[pr] for pr in range(R)], fontsize=8)
    ax.set_xlabel("时间 (us)")
    ax.set_title("丢包发生时刻(紫点线 = 微突发到达)")
    if len(drops["t"]):
        ax.legend(loc="upper right", fontsize=8)

    fig.tight_layout()
    fig.savefig(out_path, dpi=130)
    plt.close(fig)
    return out_path


def plot_grid_results(rows, out_path):
    """实验网格结果: 各策略丢包率/时延对比柱状图."""
    import csv
    scenarios = sorted({(r["incast_senders"], r["elephant_util"]) for r in rows})
    policies = []
    for r in rows:
        if r["policy"] not in policies:
            policies.append(r["policy"])
    metrics = [("drop_mice", "小请求丢包率"), ("drop_burst", "突发丢包率"),
               ("mice_p99_us", "小请求 p99 时延 (us)"),
               ("burst_fct_mean_us", "突发 FCT 均值 (us)")]
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    x = np.arange(len(scenarios))
    w = 0.8 / len(policies)
    for ax, (key, title) in zip(axes.ravel(), metrics):
        for i, pol in enumerate(policies):
            vals = []
            for sc in scenarios:
                hit = [r for r in rows
                       if r["policy"] == pol
                       and (r["incast_senders"], r["elephant_util"]) == sc]
                vals.append(hit[0][key] if hit else np.nan)
            ax.bar(x + i * w, vals, w, label=pol)
        ax.set_xticks(x + w * (len(policies) - 1) / 2)
        ax.set_xticklabels([f"K={s[0]}\n大流{int(s[1]*100)}%" for s in scenarios],
                           fontsize=8)
        ax.set_title(title)
        all_vals = [r[key] for r in rows]
        if "率" in title and any(v > 0 for v in all_vals):
            ax.set_yscale("log")
            ax.set_ylim(bottom=1e-7)
        elif "率" in title:
            ax.text(0.5, 0.5, "全部为零", transform=ax.transAxes,
                    ha="center", va="center", fontsize=12, color="gray")
        ax.legend(fontsize=8)
    fig.suptitle("共享策略对比(横轴: 突发规模 K / 大流占比)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=130)
    plt.close(fig)
    return out_path
