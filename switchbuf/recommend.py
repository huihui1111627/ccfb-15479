"""按流量组成推荐每个优先级的保留缓冲和动态门限系数.

思路(以动态门限 DT 策略为前提):
  突发超量 E = K*S - C*tau  (K 台各发 S 字节, 受害端口速率 C, tau=S/r_s)
  DT 下单个队列能占到的上限约为 alpha*(B - 其他占用)/(1+alpha),
  要装下 E 需要 alpha_mid >= E / (空闲池 - E)
  HIGH(小请求): 保留缓冲 = 突发排干期间到达的小请求字节 * 安全系数,
    系数给大(队列小, 允许大量借用), 保证不被大流/突发挤掉
  LOW(大流): 系数给小, 防止贪婪大流把共享缓冲吃光
"""

from dataclasses import dataclass

from .config import SwitchConfig, TrafficConfig, PolicyConfig


@dataclass
class Recommendation:
    policy: PolicyConfig
    notes: list

    def summary(self):
        p = self.policy
        lines = ["推荐参数 (dt 策略):"]
        names = {0: "HIGH/mice", 1: "MID/burst", 2: "LOW/elephant"}
        for pr in range(3):
            lines.append(f"  {names[pr]:<14} 保留缓冲 {p.reserved[pr]/1024:8.1f} KB  "
                         f"alpha = {p.alpha[pr]:.2f}")
        lines += ["  " + n for n in self.notes]
        return "\n".join(lines)


def recommend(cfg: SwitchConfig, tc: TrafficConfig,
              bg_occ_bytes: float = None,
              safety: float = 2.0) -> Recommendation:
    """根据流量组成给出 dt 策略参数.

    bg_occ_bytes: 无突发、无贪婪大流时测得的背景占用(可先跑一次标定仿真);
                  缺省按 2% 缓冲估计.
    """
    B = cfg.buffer_bytes
    mtu = cfg.mtu_bytes
    notes = []

    # --- 突发超量 ---
    K, S = tc.incast_senders, tc.incast_bytes_per_sender
    r_s = tc.incast_sender_rate_gbps * 125.0
    C = cfg.bytes_per_us(tc.incast_victim_port)
    tau = S / r_s
    excess = max(0.0, K * S - C * tau)
    notes.append(f"突发: K={K} 台 x {S/1024:.0f} KB, 持续 {tau:.0f} us, "
                 f"超量 {excess/1e6:.2f} MB")

    if bg_occ_bytes is None:
        bg_occ_bytes = 0.02 * B
        notes.append(f"背景占用未实测, 按 {bg_occ_bytes/1e6:.2f} MB 估计")

    # 贪婪大流在 dt(alpha_low) 下的稳态占用估计: alpha/(1+alpha) * 可用池
    alpha_low = 0.5
    ele_occ_est = alpha_low / (1 + alpha_low) * (B - bg_occ_bytes) \
        if tc.greedy_elephant_ports else 0.0
    if ele_occ_est:
        notes.append(f"贪婪大流稳态占用估计 {ele_occ_est/1e6:.2f} MB "
                     f"(alpha_low={alpha_low})")

    # --- MID: 门限系数要能让突发队列装下超量 E ---
    # DT 平衡态: q = alpha*(free - q) => q_max = alpha*free/(1+alpha) >= E
    #   => alpha >= E / (free - E)
    reserved_mid = 4 * mtu
    reserved_low = 2 * mtu
    free_pool = B - bg_occ_bytes - ele_occ_est - reserved_mid - reserved_low
    if excess <= 0:
        alpha_mid = 1.0
    elif free_pool > excess:
        alpha_mid = excess / (free_pool - excess)
    else:
        alpha_mid = 32.0
        notes.append(f"警告: 空闲池 {free_pool/1e6:.2f} MB 装不下超量 "
                     f"{excess/1e6:.2f} MB, alpha_mid 取上限")
    alpha_mid = min(max(alpha_mid, 0.5), 32.0)
    notes.append(f"alpha_mid = 超量/(空闲池-超量) = {excess/1e6:.2f} / "
                 f"({free_pool/1e6:.2f} - {excess/1e6:.2f}) MB = {alpha_mid:.2f}")

    # --- HIGH: 保留缓冲 = 突发排干期间的小请求到达量 * 安全系数 ---
    drain_us = excess / C if C > 0 else 0.0
    mice_B_per_us = tc.mice_req_per_s / 1e6 * \
        (tc.mice_size_bytes[0] + tc.mice_size_bytes[1]) / 2.0
    reserved_high = min(max(mice_B_per_us * drain_us * safety, 4 * mtu), B / 8)
    alpha_high = 8.0
    notes.append(f"突发排干 {drain_us:.0f} us 内小请求约 "
                 f"{mice_B_per_us*drain_us/1024:.1f} KB, "
                 f"保留 x{safety} = {reserved_high/1024:.1f} KB, alpha 给大")

    notes.append("大流 alpha 取小(0.5), 防止其占用过多共享缓冲挤掉别人")

    return Recommendation(
        policy=PolicyConfig(
            name="dt",
            reserved=(float(reserved_high), float(reserved_mid),
                      float(reserved_low)),
            alpha=(float(alpha_high), float(alpha_mid), float(alpha_low))),
        notes=notes)
