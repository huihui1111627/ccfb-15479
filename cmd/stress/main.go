// 压测命令：固定随机种子、多线程混合负载、事后全套核对。
package main

import (
	"flag"
	"fmt"
	"os"

	"bwtea/internal/stress"
)

func main() {
	workers := flag.Int("workers", 8, "并发线程数")
	ops := flag.Int("ops", 1200, "每个线程的操作数")
	keyspace := flag.Int("keys", 150, "键空间大小（茶样编号上界）")
	seed := flag.Int64("seed", 20261126, "固定随机种子")
	flag.Parse()

	cfg := stress.DefaultConfig()
	cfg.Seed = *seed
	cfg.Workers = *workers
	cfg.OpsPerWorker = *ops
	cfg.KeySpace = *keyspace

	fmt.Printf("启动压测: seed=%d workers=%d ops/worker=%d keyspace=%d\n",
		cfg.Seed, cfg.Workers, cfg.OpsPerWorker, cfg.KeySpace)
	report, err := stress.Run(cfg)
	if err != nil {
		fmt.Printf("❌ 核对失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ 全部核对通过")
	fmt.Printf("总操作数: %d，耗时: %v，吞吐: %.0f ops/s\n",
		report.TotalOps, report.Duration,
		float64(report.TotalOps)/report.Duration.Seconds())
	fmt.Printf("\n[1] 线性一致性: 核验键=%d 操作=%d 回溯=%d 预算兜底=%d\n",
		report.Linear.KeysChecked, report.Linear.OpsChecked,
		report.Linear.Backtracks, report.Linear.CapHits)
	fmt.Printf("[2] 最终内容对比: %s\n", report.FinalDetail)
	fmt.Printf("[3] CAS 重试率: %.2f%%（%d 次重试 / %d 次写）\n",
		report.RetryRate*100, retriesOf(report), writesOf(report))
	fmt.Printf("[4] 增量链长: 最大=%d 平均=%.2f 分桶(1/2/3-4/5-8/9-16/17+)=%v\n",
		report.ChainMax, report.ChainMean, report.ChainHist)
	fmt.Printf("[5] 延迟(μs): p50=%.1f p99=%.1f max=%.1f\n",
		report.LatencyP50Us, report.LatencyP99Us, report.LatencyMaxUs)
	fmt.Printf("[6] 映射表段: 当前=%d 累计整段归还=%d 累计整段复用=%d\n",
		report.Snap.SegmentTotal, report.Snap.SegmentReturned, report.Snap.SegmentReused)
	fmt.Printf("    分裂/合并事件: %d 条（时间线环形缓冲容量 %d）\n",
		len(report.Snap.Timeline), capOf(report))
}

func retriesOf(r *stress.Report) uint64 {
	var n uint64
	for _, w := range r.Snap.Workers {
		n += w.CASRetry
	}
	return n
}

func writesOf(r *stress.Report) uint64 {
	var n uint64
	for _, w := range r.Snap.Workers {
		n += w.Ops.Insert + w.Ops.Blind + w.Ops.Update + w.Ops.Delete
	}
	return n
}

func capOf(r *stress.Report) int {
	return 2048
}
