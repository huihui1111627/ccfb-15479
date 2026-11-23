#!/usr/bin/env python3
"""端到端演示: 仿真 -> 可视化 -> 策略对比 -> 微突发推断 -> 参数推荐.

用法:
  python3 run_demo.py all        # 全部(默认)
  python3 run_demo.py demo       # 单场景细粒度仿真 + 三联图
  python3 run_demo.py grid       # 验收: 流量组合 x 策略对比
  python3 run_demo.py infer      # 微突发推断实验
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from switchbuf.config import SwitchConfig, TrafficConfig, PolicyConfig
from switchbuf.traffic import generate_arrivals
from switchbuf.policies import Policy
from switchbuf.engine import Simulator
from switchbuf.recommend import recommend
from switchbuf.infer import infer_microbursts
from switchbuf.plots import plot_dynamics, plot_grid_results
from switchbuf.experiments import run_grid, write_rows_csv, rows_to_markdown, run_one

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")


def cmd_demo():
    """单场景: 三种策略 + 推荐参数, 各跑一次并出图."""
    os.makedirs(OUT, exist_ok=True)
    cfg = SwitchConfig()
    tc = TrafficConfig(
        duration_ms=12.0,
        elephant_util=0.3,
        mice_req_per_s=30000.0,
        incast_senders=32,
        incast_times_ms=(2.0,),     # 2ms 处一次 32 对 1 突发
        seed=1)

    # 先测背景占用用于推荐
    tc0 = TrafficConfig(**{**tc.__dict__, "incast_times_ms": (),
                           "greedy_elephant_ports": ()})
    steady = run_one(cfg, tc0, PolicyConfig(name="cs")).mean_total_occ
    reco = recommend(cfg, tc, bg_occ_bytes=steady)
    print(reco.summary())
    print()

    policies = [
        ("static", PolicyConfig(name="static")),
        ("cs", PolicyConfig(name="cs")),
        ("dt_flat", PolicyConfig(name="dt", alpha=(1, 1, 1),
                                 reserved=(4 * cfg.mtu_bytes,) * 3)),
        ("dt_reco", reco.policy),
    ]
    for pname, pc in policies:
        pol = Policy(cfg, pc)
        arr = generate_arrivals(cfg, tc)
        res = Simulator(cfg, pol).run(arr, duration_us=arr.duration_us,
                                      record_series=True)
        print(res.summary())
        path = os.path.join(OUT, f"dynamics_{pname}.png")
        plot_dynamics(res, cfg, pol, path,
                      victim_port=tc.incast_victim_port,
                      t_range_us=(1500, 8000))
        print(f"  -> {path}\n")


def cmd_grid():
    os.makedirs(OUT, exist_ok=True)
    cfg = SwitchConfig()
    t0 = time.time()
    rows = run_grid(cfg, log=lambda s: print(s, flush=True))
    csv_path = os.path.join(OUT, "grid_results.csv")
    md_path = os.path.join(OUT, "grid_results.md")
    write_rows_csv(rows, csv_path)
    md = rows_to_markdown(rows)
    with open(md_path, "w") as f:
        f.write("# 共享策略对比\n\n" + md + "\n")
    plot_grid_results(rows, os.path.join(OUT, "grid_results.png"))
    print(f"\n{md}")
    print(f"\n-> {csv_path}\n-> {md_path}\n-> {os.path.join(OUT, 'grid_results.png')}")
    print(f"总耗时 {time.time()-t0:.0f}s")


def cmd_infer():
    """较长时间仿真 + 秒级统计推断微突发."""
    os.makedirs(OUT, exist_ok=True)
    cfg = SwitchConfig()
    tc = TrafficConfig(
        duration_ms=3000.0,          # 3s -> 3 个秒级窗口
        elephant_util=0.35,
        mice_req_per_s=20000.0,
        incast_senders=32,
        incast_rate_per_s=4.0,       # 平均每秒 4 次突发
        seed=7)
    arr = generate_arrivals(cfg, tc)
    print(f"到达报文 {len(arr)} 个, incast 事件 {len(arr.incast_events)} 次")
    t0 = time.time()
    res = Simulator(cfg, Policy(cfg, PolicyConfig(
        name="dt", alpha=(8, 2, 0.5),
        reserved=(64 * 1024, 4 * 1500, 2 * 1500)))).run(
            arr, duration_us=arr.duration_us)
    print(f"仿真耗时 {time.time()-t0:.1f}s")
    print(res.summary())
    ir = infer_microbursts(cfg, res, window_s=1.0)
    print(ir.summary())
    print("\n逐窗口明细 (仅列有丢包或有突发的窗口):")
    print(f"{'端口':>4} {'窗口':>4} {'利用率':>7} {'丢包KB':>9} "
          f"{'检出':>4} {'估计超量MB':>10} {'实际超量MB':>10}")
    for w in ir.windows:
        if w["drop_B"] > 0 or w["has_burst"]:
            print(f"{w['port']:>4} {w['win']:>4} {w['util']:>7.1%} "
                  f"{w['drop_B']/1024:>9.1f} "
                  f"{'Y' if w['flagged'] else '-':>4} "
                  f"{w['est_excess_B']/1e6:>10.2f} "
                  f"{w['actual_excess_B']/1e6:>10.2f}")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd in ("demo", "all"):
        cmd_demo()
    if cmd in ("grid", "all"):
        cmd_grid()
    if cmd in ("infer", "all"):
        cmd_infer()


if __name__ == "__main__":
    main()
