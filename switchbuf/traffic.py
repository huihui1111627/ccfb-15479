"""流量生成(NumPy 向量化): 微突发 incast、大流、时延敏感小请求.

大流(elephant)按流体建模: 每端口恒定字节速率, 由引擎按 us 注入、
可部分准入(模拟贪婪 TCP 持续占缓冲的行为), 不产生逐个报文.
小请求(mice)和微突发(incast burst)按报文建模, 输出统一到达事件流.
"""

from dataclasses import dataclass, field
import numpy as np

from .config import MICE, BURST, ELEPHANT, SwitchConfig, TrafficConfig


@dataclass
class Arrivals:
    """到达事件(并行数组, 按 t 升序) + 大流流体速率."""

    t: np.ndarray        # int64, 到达时刻 us
    port: np.ndarray     # int32
    prio: np.ndarray     # int32
    size: np.ndarray     # int32, 字节
    fid: np.ndarray      # int64, 请求/突发事件 id
    cls: np.ndarray      # int32, MICE/BURST
    elephant_bpus: np.ndarray = None   # (n_ports,) 大流流体速率 B/us
    duration_us: int = 0

    # 元数据
    req_info: dict = field(default_factory=dict)   # fid -> (cls, t_start_us, n_pkts)
    incast_events: list = field(default_factory=list)  # dict(t_us, port, senders, ...)

    def __len__(self):
        return len(self.t)


def elephant_rates(cfg: SwitchConfig, tc: TrafficConfig) -> np.ndarray:
    """每端口大流流体速率 (B/us).

    - elephant_util: 背景大流占各端口容量比例(elephant_ports 指定的端口)
    - greedy_elephant_ports: 贪婪大流端口, 以 greedy_factor 倍端口速率发送,
      持续尝试占满缓冲(模拟瓶颈链路上的 TCP 大流)
    """
    rate = np.zeros(cfg.n_ports)
    eports = tc.elephant_ports or tuple(range(cfg.n_ports))
    for p in eports:
        rate[p] += cfg.bytes_per_us(p) * tc.elephant_util
    for p in tc.greedy_elephant_ports:
        rate[p] += cfg.bytes_per_us(p) * tc.greedy_factor
    return rate


def _emit_mice(cfg: SwitchConfig, tc: TrafficConfig, rng, T_us, fid0):
    """小请求: 泊松到达, 每个请求 1~若干 MTU 报文, 报文间隔 1us."""
    ts, ports, sizes, fids = [], [], [], []
    req_info = {}
    fid = fid0
    lo, hi = tc.mice_size_bytes
    T_s = T_us / 1e6
    for p in range(cfg.n_ports):
        n_req = rng.poisson(tc.mice_req_per_s * T_s)
        if n_req == 0:
            continue
        t_req = np.sort(rng.uniform(0, T_us, n_req))
        nbytes = rng.uniform(lo, hi, n_req)
        npkts = np.maximum(1, np.ceil(nbytes / cfg.mtu_bytes).astype(np.int64))
        offs = np.arange(npkts.max())
        t_all = (t_req[:, None] + offs[None, :]).ravel()
        mask = (offs[None, :] < npkts[:, None]).ravel()
        t_all = t_all[mask]
        p_all = np.full(len(t_all), p, dtype=np.int32)
        rem = nbytes - (npkts - 1) * cfg.mtu_bytes
        size_rep = np.full(int(npkts.sum()), cfg.mtu_bytes, dtype=np.int32)
        size_rep[np.cumsum(npkts) - 1] = rem.astype(np.int32)
        fid_all = np.repeat(np.arange(fid, fid + n_req, dtype=np.int64), npkts)
        for i in range(n_req):
            req_info[fid + i] = (MICE, int(t_req[i]), int(npkts[i]))
        fid += n_req
        ts.append(t_all.astype(np.int64))
        ports.append(p_all)
        sizes.append(size_rep)
        fids.append(fid_all)
    if not ts:
        z = np.zeros(0, dtype=np.int64)
        return z, z.astype(np.int32), z.astype(np.int32), z, req_info, fid
    return (np.concatenate(ts), np.concatenate(ports), np.concatenate(sizes),
            np.concatenate(fids), req_info, fid)


def _emit_incast(cfg: SwitchConfig, tc: TrafficConfig, rng, T_us, fid0):
    """微突发: K 台服务器同时向一个端口各发 S 字节, 持续数百 us."""
    if tc.incast_times_ms:
        times_us = np.asarray(tc.incast_times_ms, dtype=np.float64) * 1e3
    else:
        n = rng.poisson(tc.incast_rate_per_s * T_us / 1e6)
        times_us = np.sort(rng.uniform(0.05 * T_us, 0.95 * T_us, max(n, 1)))
    K, S = tc.incast_senders, tc.incast_bytes_per_sender
    r_s = tc.incast_sender_rate_gbps * 125.0           # B/us per sender
    pkt_int_us = cfg.mtu_bytes / r_s                    # 报文间隔
    npkts = int(np.ceil(S / cfg.mtu_bytes))
    victim = tc.incast_victim_port
    C = cfg.bytes_per_us(victim)

    ts, ports, sizes, fids = [], [], [], []
    req_info = {}
    events = []
    fid = fid0
    for t0 in times_us:
        jitter = rng.uniform(0, tc.incast_jitter_us, K)
        start = t0 + jitter
        t_s = (start[:, None] + np.arange(npkts)[None, :] * pkt_int_us).ravel()
        s_last = S - (npkts - 1) * cfg.mtu_bytes
        s_s = np.tile(np.concatenate([
            np.full(npkts - 1, cfg.mtu_bytes, dtype=np.int32), [s_last]]), K)
        ts.append(t_s.astype(np.int64))
        ports.append(np.full(len(t_s), victim, dtype=np.int32))
        sizes.append(s_s)
        fids.append(np.full(len(t_s), fid, dtype=np.int64))
        req_info[fid] = (BURST, int(t0), K * npkts)
        offered = K * S
        tau_us = S / r_s                       # 突发持续时间(数百 us 量级)
        excess = max(0.0, offered - C * tau_us)  # 端口来不及发出的部分
        events.append(dict(t_us=float(t0), port=victim, senders=K,
                           bytes_per_sender=S, offered_bytes=float(offered),
                           duration_us=float(tau_us), excess_bytes=float(excess)))
        fid += 1
    if not ts:
        z = np.zeros(0, dtype=np.int64)
        return z, z.astype(np.int32), z.astype(np.int32), z, req_info, events
    return (np.concatenate(ts), np.concatenate(ports), np.concatenate(sizes),
            np.concatenate(fids), req_info, events)


def generate_arrivals(cfg: SwitchConfig, tc: TrafficConfig) -> Arrivals:
    """生成完整到达事件流(报文级: 小请求+突发; 流体级: 大流速率)."""
    rng = np.random.default_rng(tc.seed)
    T_us = int(tc.duration_ms * 1e3)

    t_m, p_m, s_m, f_m, info_m, fid = _emit_mice(cfg, tc, rng, T_us, fid0=1)
    t_b, p_b, s_b, f_b, info_b, events = _emit_incast(cfg, tc, rng, T_us, fid0=fid)

    t = np.concatenate([t_m, t_b])
    port = np.concatenate([p_m, p_b])
    size = np.concatenate([s_m, s_b])
    fid_a = np.concatenate([f_m, f_b])
    cls = np.concatenate([
        np.full(len(t_m), MICE, dtype=np.int32),
        np.full(len(t_b), BURST, dtype=np.int32)])
    prio = np.where(cls == MICE, 0, 1).astype(np.int32)

    order = np.argsort(t, kind="stable")
    req_info = {}
    req_info.update(info_m)
    req_info.update(info_b)
    return Arrivals(t=t[order], port=port[order], prio=prio[order],
                    size=size[order], fid=fid_a[order], cls=cls[order],
                    elephant_bpus=elephant_rates(cfg, tc),
                    duration_us=T_us,
                    req_info=req_info, incast_events=events)
