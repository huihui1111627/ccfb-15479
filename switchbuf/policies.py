"""缓冲区共享策略(准入控制).

所有策略共享同一接口: admit(q_occ, total_occ, size, prio) -> bool
  q_occ     该 (端口,优先级) 队列当前占用字节
  total_occ 全部队列总占用字节
  size      待准入报文字节
"""

from .config import SwitchConfig, PolicyConfig


class Policy:
    def __init__(self, cfg: SwitchConfig, pc: PolicyConfig):
        self.cfg = cfg
        self.pc = pc
        self.name = pc.name
        self.reserved = pc.reserved_arr()
        self.alpha = pc.alpha_arr()
        if pc.name == "static":
            # 静态均分: 每个 (端口,优先级) 队列硬上限
            self.cap = cfg.buffer_bytes / cfg.n_queues

    def admit(self, q_occ, total_occ, size, prio):
        B = self.cfg.buffer_bytes
        if self.name == "static":
            return q_occ + size <= self.cap
        if self.name == "cs":
            return total_occ + size <= B
        if self.name == "dt":
            # 保留缓冲: 占用低于 reserved 时总是放行(前提总量没爆)
            if q_occ < self.reserved[prio]:
                return total_occ + size <= B
            # 动态门限: 上限 = alpha * 当前空闲缓冲
            free = B - total_occ
            if free <= 0:
                return False
            return q_occ + size <= self.alpha[prio] * free
        raise ValueError(f"unknown policy {self.name}")

    def admit_fluid(self, q_occ, total_occ, prio):
        """流体(大流)准入: 返回当前允许进入的字节数(可部分准入)."""
        B = self.cfg.buffer_bytes
        if self.name == "static":
            return max(0.0, self.cap - q_occ)
        if self.name == "cs":
            return max(0.0, B - total_occ)
        # dt: 上限取 max(保留缓冲, 动态门限)
        lim = max(self.reserved[prio],
                  self.alpha[prio] * max(0.0, B - total_occ))
        return max(0.0, min(lim - q_occ, B - total_occ))

    def threshold(self, prio, total_occ):
        """dt 策略当前门限(用于绘图); 其他策略返回等效硬上限."""
        B = self.cfg.buffer_bytes
        if self.name == "static":
            return self.cap
        if self.name == "cs":
            return B - total_occ
        return self.alpha[prio] * max(0.0, B - total_occ)


def make_policy(cfg: SwitchConfig, name: str, **kw) -> Policy:
    return Policy(cfg, PolicyConfig(name=name, **kw))
