"""从秒级端口统计推断微突发.

模拟运维真实处境: 只有每个端口、每个监控窗口(默认 1s)的
  - 发出字节数(链路利用率)
  - 丢弃字节数
看不到微秒级队列占用. 用启发式规则推断:
  1. 检出: 窗口内有丢包 且 平均利用率不高(< util_thresh)
     => 平均利用率低却丢包, 只能是瞬时微突发打满缓冲
  2. 规模估计: 突发超量字节 ≈ 丢弃字节 + 缓冲吸收量(≈共享缓冲总量)
     突发峰值速率 ≈ 端口速率 + 超量字节 / 假定突发时长
与仿真 ground truth(incast 事件列表)对照, 给出检出率/误报率/规模误差.
"""

from dataclasses import dataclass

import numpy as np

from .config import SwitchConfig


@dataclass
class InferResult:
    windows: list          # 每窗口 dict: port, win, util, drop_B, flagged, est_excess_B, ...
    detection_rate: float  # 有突发窗口中被检出的比例
    false_positive_rate: float  # 无突发窗口被误报的比例
    scale_rel_err: float   # 规模估计相对误差(中位数)
    n_burst_windows: int
    n_clean_windows: int

    def summary(self):
        return (f"微突发推断: 检出率 {self.detection_rate*100:.1f}% "
                f"({self.n_burst_windows} 个含突发窗口), "
                f"误报率 {self.false_positive_rate*100:.1f}% "
                f"({self.n_clean_windows} 个无突发窗口), "
                f"规模估计相对误差中位数 {self.scale_rel_err*100:.1f}%")


def infer_microbursts(cfg: SwitchConfig, res, window_s=1.0,
                      util_thresh=0.7, assumed_burst_us=300.0) -> InferResult:
    """从端口统计推断微突发并对照 ground truth.

    res: SimResult (需含 port_served_B_per_ms / port_dropped_B_per_ms / incast_events)
    """
    served = res.port_served_B_per_ms      # (P, n_ms)
    dropped = res.port_dropped_B_per_ms
    P, n_ms = served.shape
    win_ms = int(window_s * 1e3)
    n_win = n_ms // win_ms
    if n_win == 0:
        n_win = 1
        win_ms = n_ms

    # ground truth: 每 (port, window) 的突发超量字节
    gt_excess = np.zeros((P, n_win))
    for ev in res.incast_events or []:
        w = int(ev["t_us"] / 1e3) // win_ms
        if w < n_win:
            gt_excess[ev["port"], w] += ev["excess_bytes"]

    B = cfg.buffer_bytes
    windows = []
    n_burst = n_clean = 0
    n_detect = n_fp = 0
    rel_errs = []
    for p in range(P):
        cap_B_per_win = cfg.bytes_per_us(p) * win_ms * 1e3
        for w in range(n_win):
            s = served[p, w * win_ms:(w + 1) * win_ms].sum()
            d = dropped[p, w * win_ms:(w + 1) * win_ms].sum()
            util = s / cap_B_per_win if cap_B_per_win > 0 else 0.0
            flagged = bool(d > 0 and util < util_thresh)
            est_excess = float(d + B) if flagged else 0.0
            est_peak_gbps = (cfg.port_rates_gbps[p]
                             + est_excess / max(assumed_burst_us, 1e-9) / 125.0
                             if flagged else 0.0)
            actual = gt_excess[p, w]
            has_burst = actual > 0
            if has_burst:
                n_burst += 1
                if flagged:
                    n_detect += 1
                    rel_errs.append(abs(est_excess - actual) / actual)
            else:
                n_clean += 1
                if flagged:
                    n_fp += 1
            windows.append(dict(port=p, win=w, util=float(util),
                                drop_B=float(d), flagged=flagged,
                                est_excess_B=est_excess,
                                est_peak_gbps=float(est_peak_gbps),
                                actual_excess_B=float(actual),
                                has_burst=bool(has_burst)))
    return InferResult(
        windows=windows,
        detection_rate=n_detect / n_burst if n_burst else float("nan"),
        false_positive_rate=n_fp / n_clean if n_clean else float("nan"),
        scale_rel_err=float(np.median(rel_errs)) if rel_errs else float("nan"),
        n_burst_windows=n_burst, n_clean_windows=n_clean)
