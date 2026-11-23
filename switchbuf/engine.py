"""微秒级时间步仿真引擎.

每 1us 一个时间步:
  1. 注入该时刻到达的报文(小请求/突发, 含 RTO 重传)
  2. 大流按流体注入(每端口恒定速率, 按策略部分准入)
  3. 准入控制: 拒绝的报文丢弃(小请求/突发按 RTO 重传), 流体截断
  4. 各端口按速率、严格优先级调度: HIGH/MID 报文队列 -> LOW 大流流体队列
"""

import heapq
from collections import deque
from dataclasses import dataclass

import numpy as np

from .config import MICE, BURST, ELEPHANT, CLS_NAMES, SwitchConfig
from .policies import Policy
from .traffic import Arrivals

PRIO_ELEPHANT = 2


@dataclass
class SimResult:
    policy_name: str
    duration_us: int
    arrived_bytes: np.ndarray          # 每类到达字节
    dropped_bytes: np.ndarray          # 每类丢弃字节
    mice_lat_us: np.ndarray            # 完成的小请求时延
    mice_failed: int
    burst_fct_us: np.ndarray           # 每个 incast 事件的完成时间
    burst_failed: int
    peak_total_occ: float
    peak_prio_occ: np.ndarray
    mean_total_occ: float
    drops: dict = None                 # t, port, prio, size, cls (np arrays)
    series: dict = None                # record_series=True 时: t, total, prio_occ, q_occ
    port_served_B_per_ms: np.ndarray = None  # (n_ports, n_ms) 供秒级统计推断
    port_dropped_B_per_ms: np.ndarray = None
    incast_events: list = None

    def drop_rate(self, cls):
        a = self.arrived_bytes[cls]
        return float(self.dropped_bytes[cls] / a) if a > 0 else 0.0

    def mice_pct(self, q):
        if len(self.mice_lat_us) == 0:
            return float("nan")
        return float(np.percentile(self.mice_lat_us, q))

    def burst_pct(self, q):
        if len(self.burst_fct_us) == 0:
            return float("nan")
        return float(np.percentile(self.burst_fct_us, q))

    def summary(self):
        lines = [f"[{self.policy_name}] 时长 {self.duration_us/1e3:.0f} ms"]
        for c in (MICE, BURST, ELEPHANT):
            lines.append(
                f"  {CLS_NAMES[c]:<14} 丢包率 {self.drop_rate(c)*100:8.4f}%  "
                f"({self.dropped_bytes[c]/1e6:.2f} MB / {self.arrived_bytes[c]/1e6:.1f} MB)")
        lines.append(f"  小请求时延 us: p50={self.mice_pct(50):.0f} "
                     f"p99={self.mice_pct(99):.0f} p99.9={self.mice_pct(99.9):.0f} "
                     f"失败 {self.mice_failed}")
        mf = np.mean(self.burst_fct_us) if len(self.burst_fct_us) else float("nan")
        lines.append(f"  突发FCT us:  mean={mf:.0f} p99={self.burst_pct(99):.0f} "
                     f"失败 {self.burst_failed}")
        lines.append(f"  缓冲占用: 峰值 {self.peak_total_occ/1e6:.2f} MB, "
                     f"均值 {self.mean_total_occ/1e6:.2f} MB")
        return "\n".join(lines)


class Simulator:
    def __init__(self, cfg: SwitchConfig, policy: Policy):
        self.cfg = cfg
        self.policy = policy
        P, R = cfg.n_ports, cfg.n_priorities
        self.queues = [[deque() for _ in range(R)] for _ in range(P)]
        self.q_occ = np.zeros((P, R), dtype=np.float64)
        self.total_occ = 0.0
        self.bpus = np.array([cfg.bytes_per_us(p) for p in range(P)])

    def run(self, arr: Arrivals, duration_us=None, record_series=False,
            measure_from_us=0):
        cfg = self.cfg
        T = int(duration_us) if duration_us else (
            int(arr.t.max()) + 2 if len(arr) else 1)
        P, R = cfg.n_ports, cfg.n_priorities
        ele_rate = arr.elephant_bpus if arr.elephant_bpus is not None \
            else np.zeros(P)

        req_state = {f: [n, t0, c] for f, (c, t0, n) in arr.req_info.items()}
        failed_reqs = set()
        retrans_heap = []  # (t_us, retries, fid, size, port, prio, cls)

        arrived_B = np.zeros(3, dtype=np.int64)
        dropped_B = np.zeros(3, dtype=np.int64)
        mice_lat, burst_fct = [], []
        mice_failed = burst_failed = 0

        n_ms = T // 1000 + 1
        served_ms = np.zeros((P, n_ms), dtype=np.int64)
        dropms = np.zeros((P, n_ms), dtype=np.int64)
        drop_log = {k: [] for k in ("t", "port", "prio", "size", "cls")}

        if record_series:
            s_total = np.zeros(T, dtype=np.float64)
            s_prio = np.zeros((T, R), dtype=np.float64)
            s_q = np.zeros((T, P, R), dtype=np.float32)
        occ_accum = 0.0
        peak_total = 0.0
        peak_prio = np.zeros(R)

        ai = 0
        N = len(arr)
        a_t, a_p, a_pr = arr.t, arr.port, arr.prio
        a_s, a_f, a_c = arr.size, arr.fid, arr.cls
        admit = self.policy.admit
        admit_fluid = self.policy.admit_fluid
        queues, q_occ = self.queues, self.q_occ
        rto = int(cfg.rto_us)
        max_re = cfg.max_retrans

        def on_drop(c, f, s, p, pr, t, r):
            nonlocal mice_failed, burst_failed
            dropped_B[c] += s
            dropms[p, t // 1000] += s
            drop_log["t"].append(t); drop_log["port"].append(p)
            drop_log["prio"].append(pr); drop_log["size"].append(s)
            drop_log["cls"].append(c)
            if r + 1 > max_re:
                # 单个报文重传次数耗尽 -> 所属请求/事件失败
                if f not in failed_reqs:
                    failed_reqs.add(f)
                    st = req_state.get(f)
                    if st is not None and st[0] > 0:
                        st[0] = 0
                        if c == MICE:
                            mice_failed += 1
                        else:
                            burst_failed += 1
            else:
                heapq.heappush(retrans_heap, (t + rto, r + 1, f, s, p, pr, c))

        for t in range(T):
            # ---- 1a. 报文到达(小请求/突发) ----
            while ai < N and a_t[ai] == t:
                p, pr, s, f, c = a_p[ai], a_pr[ai], a_s[ai], a_f[ai], a_c[ai]
                ai += 1
                arrived_B[c] += s
                if admit(q_occ[p, pr], self.total_occ, s, pr):
                    queues[p][pr].append([s, t, f, c])
                    q_occ[p, pr] += s
                    self.total_occ += s
                else:
                    on_drop(c, f, s, p, pr, t, 0)
            # ---- 1b. 重传到达 ----
            while retrans_heap and retrans_heap[0][0] <= t:
                _, r, f, s, p, pr, c = heapq.heappop(retrans_heap)
                arrived_B[c] += s
                if admit(q_occ[p, pr], self.total_occ, s, pr):
                    queues[p][pr].append([s, t, f, c])
                    q_occ[p, pr] += s
                    self.total_occ += s
                else:
                    on_drop(c, f, s, p, pr, t, r)
            # ---- 1c. 大流流体注入(可部分准入) ----
            for p in range(P):
                offered = ele_rate[p]
                if offered <= 0:
                    continue
                arrived_B[ELEPHANT] += int(offered)
                allowed = admit_fluid(q_occ[p, PRIO_ELEPHANT],
                                      self.total_occ, PRIO_ELEPHANT)
                admitted = offered if offered <= allowed else allowed
                if admitted > 0:
                    q_occ[p, PRIO_ELEPHANT] += admitted
                    self.total_occ += admitted
                d = offered - admitted
                if d > 0:
                    dropped_B[ELEPHANT] += int(d)
                    dropms[p, t // 1000] += int(d)
                    if record_series:
                        drop_log["t"].append(t); drop_log["port"].append(p)
                        drop_log["prio"].append(PRIO_ELEPHANT)
                        drop_log["size"].append(int(d))
                        drop_log["cls"].append(ELEPHANT)

            # ---- 2. 各端口严格优先级调度 ----
            for p in range(P):
                budget = self.bpus[p]
                if budget <= 0:
                    continue
                for pr in range(R):
                    if budget <= 0:
                        break
                    if pr == PRIO_ELEPHANT:
                        # 流体大流队列
                        occ = q_occ[p, PRIO_ELEPHANT]
                        if occ > 0:
                            serve = occ if occ <= budget else budget
                            q_occ[p, PRIO_ELEPHANT] -= serve
                            self.total_occ -= serve
                            served_ms[p, t // 1000] += int(serve)
                            budget -= serve
                        continue
                    q = queues[p][pr]
                    while budget > 0 and q:
                        head = q[0]
                        rem = head[0]
                        if rem <= budget:
                            q.popleft()
                            budget -= rem
                            q_occ[p, pr] -= rem
                            self.total_occ -= rem
                            served_ms[p, t // 1000] += rem
                            f, c = head[2], head[3]
                            st = req_state.get(f)
                            if st is not None and st[0] > 0:
                                st[0] -= 1
                                if st[0] == 0 and st[1] >= measure_from_us:
                                    lat = t - st[1]
                                    if c == MICE:
                                        mice_lat.append(lat)
                                    else:
                                        burst_fct.append(lat)
                        else:
                            head[0] = rem - budget
                            q_occ[p, pr] -= budget
                            self.total_occ -= budget
                            served_ms[p, t // 1000] += int(budget)
                            budget = 0.0

            # ---- 3. 记录 ----
            if self.total_occ > peak_total:
                peak_total = self.total_occ
            occ_accum += self.total_occ
            if record_series:
                s_total[t] = self.total_occ
                row = q_occ.sum(axis=0)
                s_prio[t] = row
                s_q[t] = q_occ
                np.maximum(peak_prio, row, out=peak_prio)

        res = SimResult(
            policy_name=self.policy.name,
            duration_us=T,
            arrived_bytes=arrived_B, dropped_bytes=dropped_B,
            mice_lat_us=np.asarray(mice_lat, dtype=np.float64),
            mice_failed=mice_failed,
            burst_fct_us=np.asarray(burst_fct, dtype=np.float64),
            burst_failed=burst_failed,
            peak_total_occ=peak_total, peak_prio_occ=peak_prio,
            mean_total_occ=occ_accum / T,
            drops={k: np.asarray(v) for k, v in drop_log.items()},
            port_served_B_per_ms=served_ms,
            port_dropped_B_per_ms=dropms,
            incast_events=arr.incast_events,
        )
        if record_series:
            res.series = dict(t=np.arange(T), total=s_total, prio_occ=s_prio,
                              q_occ=s_q, n_ports=P, n_prio=R)
        return res
