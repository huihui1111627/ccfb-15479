"""验收实验: 多种流量组合 x 多种共享策略, 输出对比表."""

import csv
import itertools
import time

import numpy as np

from .config import SwitchConfig, TrafficConfig, PolicyConfig
from .traffic import generate_arrivals
from .policies import Policy
from .engine import Simulator
from .recommend import recommend


def run_one(cfg: SwitchConfig, tc: TrafficConfig, pc: PolicyConfig,
            record_series=False):
    arr = generate_arrivals(cfg, tc)
    sim = Simulator(cfg, Policy(cfg, pc))
    return sim.run(arr, duration_us=arr.duration_us,
                   record_series=record_series)


def _steady_occ(cfg: SwitchConfig, tc: TrafficConfig):
    """背景占用标定: 关掉突发和贪婪大流, 测稳态总占用."""
    tc0 = TrafficConfig(**{**tc.__dict__, "incast_rate_per_s": 0.0,
                           "incast_times_ms": (),
                           "greedy_elephant_ports": (),
                           "duration_ms": 20.0})
    res = run_one(cfg, tc0, PolicyConfig(name="cs"))
    return res.mean_total_occ


def result_row(res, **meta):
    return dict(
        mice_p50_us=res.mice_pct(50), mice_p99_us=res.mice_pct(99),
        mice_p999_us=res.mice_pct(99.9),
        drop_mice=res.drop_rate(0), drop_burst=res.drop_rate(1),
        drop_elephant=res.drop_rate(2),
        burst_fct_mean_us=float(np.mean(res.burst_fct_us)) if len(res.burst_fct_us) else float("nan"),
        burst_fct_p99_us=res.burst_pct(99),
        peak_occ_MB=res.peak_total_occ / 1e6,
        mean_occ_MB=res.mean_total_occ / 1e6,
        mice_failed=res.mice_failed, burst_failed=res.burst_failed,
        **meta)


def run_grid(cfg: SwitchConfig, senders_list=(8, 16, 32),
             elephant_utils=(0.1, 0.3, 0.5), duration_ms=60.0,
             seed=0, log=print):
    """流量组合网格: 静态均分 / 完全共享 / 动态门限(平系数) / 动态门限(推荐)."""
    rows = []
    for K, eu in itertools.product(senders_list, elephant_utils):
        tc = TrafficConfig(duration_ms=duration_ms,
                           elephant_util=eu,
                           incast_senders=K,
                           incast_rate_per_s=50.0,   # 60ms 内约 3 次突发
                           seed=seed)
        steady = _steady_occ(cfg, tc)
        reco = recommend(cfg, tc, bg_occ_bytes=steady)
        policies = [
            ("static", PolicyConfig(name="static")),
            ("cs", PolicyConfig(name="cs")),
            ("dt_flat", PolicyConfig(name="dt", alpha=(1, 1, 1),
                                     reserved=(4 * cfg.mtu_bytes,) * 3)),
            ("dt_reco", reco.policy),
        ]
        for pname, pc in policies:
            t0 = time.time()
            res = run_one(cfg, tc, pc)
            row = result_row(res, policy=pname, incast_senders=K,
                             elephant_util=eu)
            rows.append(row)
            log(f"K={K:3d} 大流={eu:.0%} {pname:8s} "
                f"丢包(mice/burst/ele)={row['drop_mice']:.2e}/"
                f"{row['drop_burst']:.2e}/{row['drop_elephant']:.2e} "
                f"mice_p99={row['mice_p99_us']:.0f}us "
                f"FCT={row['burst_fct_mean_us']:.0f}us "
                f"峰值={row['peak_occ_MB']:.1f}MB "
                f"({time.time()-t0:.1f}s)")
    return rows


def write_rows_csv(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def rows_to_markdown(rows):
    cols = ["incast_senders", "elephant_util", "policy",
            "drop_mice", "drop_burst", "drop_elephant",
            "mice_p99_us", "burst_fct_mean_us", "peak_occ_MB"]
    hdr = ["K", "大流占比", "策略", "小请求丢包", "突发丢包", "大流丢包",
           "小请求p99(us)", "突发FCT均值(us)", "峰值占用(MB)"]
    lines = ["| " + " | ".join(hdr) + " |",
             "|" + "---|" * len(hdr)]
    for r in rows:
        lines.append("| {} | {:.0%} | {} | {:.2e} | {:.2e} | {:.2e} "
                     "| {:.0f} | {:.0f} | {:.2f} |".format(
                         r["incast_senders"], r["elephant_util"], r["policy"],
                         r["drop_mice"], r["drop_burst"], r["drop_elephant"],
                         r["mice_p99_us"], r["burst_fct_mean_us"],
                         r["peak_occ_MB"]))
    return "\n".join(lines)
