"""switchbuf — 数据中心交换机共享缓冲区仿真与调优工具."""

from .config import SwitchConfig, TrafficConfig, PolicyConfig
from .engine import Simulator, SimResult
from .traffic import generate_arrivals, Arrivals

__all__ = [
    "SwitchConfig",
    "TrafficConfig",
    "PolicyConfig",
    "Simulator",
    "SimResult",
    "generate_arrivals",
    "Arrivals",
]
