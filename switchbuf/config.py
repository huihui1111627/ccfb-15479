"""配置定义:交换机、流量、缓冲区共享策略.

优先级约定(数值越小优先级越高,严格优先级调度):
  0 = HIGH   时延敏感小请求 (mice)
  1 = MID    多对一汇聚微突发 (incast/burst)
  2 = LOW    长期占带宽的大流 (elephant)
"""

from dataclasses import dataclass, field
import numpy as np

# 流量类别
MICE, BURST, ELEPHANT = 0, 1, 2
CLS_NAMES = {MICE: "mice(小请求)", BURST: "burst(微突发)", ELEPHANT: "elephant(大流)"}
# 类别 -> 优先级队列
CLS2PRIO = {MICE: 0, BURST: 1, ELEPHANT: 2}
PRIO_NAMES = {0: "HIGH/mice", 1: "MID/burst", 2: "LOW/elephant"}


@dataclass
class SwitchConfig:
    """交换机参数."""

    port_rates_gbps: tuple = (10, 25, 25, 40, 100, 100)  # 各端口速率不同
    n_priorities: int = 3
    buffer_bytes: int = 32 * 1024 ** 2   # 共享缓冲总量(几十兆量级,所有端口共享)
    dt_us: float = 1.0                    # 微秒级时间步
    mtu_bytes: int = 1500
    rto_us: float = 1000.0                # 丢包重传超时(简化:固定 RTO)
    max_retrans: int = 7                  # 超过则请求失败

    @property
    def n_ports(self):
        return len(self.port_rates_gbps)

    @property
    def n_queues(self):
        return self.n_ports * self.n_priorities

    def bytes_per_us(self, port):
        return self.port_rates_gbps[port] * 125.0  # Gbps -> B/us


@dataclass
class TrafficConfig:
    """流量组成."""

    duration_ms: float = 100.0

    # 大流(流体建模): elephant_util 部分为恒定速率背景大流;
    # greedy_elephant_ports 上的大流以 greedy_factor 倍端口速率贪婪发送,
    # 持续试图占满缓冲(模拟瓶颈链路上的 TCP 大流)
    elephant_util: float = 0.3           # 背景大流占端口容量比例
    elephant_ports: tuple = ()           # 背景大流端口; 空 = 全部
    greedy_elephant_ports: tuple = (5,)  # 贪婪大流端口(默认非受害端口)
    greedy_factor: float = 1.3

    # 小请求: 每端口泊松到达
    mice_req_per_s: float = 20000.0      # 每端口请求率
    mice_size_bytes: tuple = (1500, 12000)  # 请求大小范围(均匀)

    # 微突发(incast): 多对一汇聚
    incast_victim_port: int = 4
    incast_senders: int = 32             # K: 同时发送的服务器数
    incast_bytes_per_sender: int = 1024 * 1024  # 每台发送量(25Gbps 下持续约 330us)
    incast_sender_rate_gbps: float = 25.0
    incast_jitter_us: float = 50.0       # 发送起点抖动
    incast_times_ms: tuple = ()          # 显式指定突发时刻; 空则按泊松
    incast_rate_per_s: float = 20.0      # 泊松速率(显式时刻为空时生效)

    seed: int = 0


@dataclass
class PolicyConfig:
    """缓冲区共享策略.

    name:
      static  静态均分: 每队列硬上限 = buffer / n_queues
      cs      完全共享: 只要总占用未满就放行
      dt      动态门限: 队列上限 = alpha[prio] * 当前空闲缓冲, 另有每优先级保留缓冲
    """

    name: str = "dt"
    reserved: tuple = (0.0, 0.0, 0.0)    # 每优先级保留最小缓冲(字节)
    alpha: tuple = (1.0, 1.0, 1.0)       # 每优先级动态门限系数

    def reserved_arr(self):
        return np.asarray(self.reserved, dtype=np.float64)

    def alpha_arr(self):
        return np.asarray(self.alpha, dtype=np.float64)
