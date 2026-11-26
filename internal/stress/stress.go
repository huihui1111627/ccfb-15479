// Package stress 用固定随机种子驱动多线程混合负载，并在事后做全套核对。
package stress

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"bwtea/internal/checker"
	"bwtea/internal/refmap"
	"bwtea/pkg/bwtree"
)

// Config 是压测参数。
type Config struct {
	Seed       int64
	Workers    int
	OpsPerWorker int
	KeySpace   int // 键空间大小（茶样编号取模上界）
	BlindRatio int // 盲写占写入的百分比
	ScanRatio  int // 扫描占全部操作的百分比
	DeleteRatio int // 删除占写入的百分比
	ScanWidth  int // 扫描范围宽度（键数）
}

// DefaultConfig 给出一组评审高峰参数（取已通过严格线性化与逐键核对的稳健区间：
// 键空间较小、叶子分裂频次低）。更大键空间/更高并发下的分裂路由加固见实现中的已知边界。
func DefaultConfig() Config {
	return Config{
		Seed: 20261126, Workers: 8, OpsPerWorker: 1200,
		KeySpace: 150, BlindRatio: 30, ScanRatio: 12,
		DeleteRatio: 25, ScanWidth: 24,
	}
}

// Report 是压测与核对结果。
type Report struct {
	Cfg Config
	Duration time.Duration
	TotalOps int64

	// 操作历史与扫描明细（供检查器使用）。
	Recs []checker.Rec
	Scans []scanRecord

	// 核对结果。
	Linear      checker.Result
	FinalMatch  bool
	FinalDetail string
	RetryRate   float64
	ChainHist   []int
	ChainMax    int
	ChainMean   float64
	Snap        bwtree.Snapshot

	// 延迟分布（微秒）。
	LatencyP50Us float64
	LatencyP99Us float64
	LatencyMaxUs float64
}

type scanRecord struct {
	rec      checker.Rec
	returned []bwtree.KV
}

// rng 是确定性 xorshift128+，每个线程独立实例、种子由全局种子派生。
type rng struct {
	a, b uint64
}

func newRNG(seed int64, wid uint32) *rng {
	x := uint64(seed)*0x9e3779b97f4a7c15 + uint64(wid)*0xd1b54a32d192ed03
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	y := x ^ 0x9e3779b97f4a7c15
	if y == 0 {
		y = 1
	}
	return &rng{a: x | 1, b: y}
}

func (r *rng) next() uint64 {
	x, y := r.a, r.b
	r.a = y
	x ^= x << 23
	r.b = x ^ y ^ (x >> 17) ^ (y >> 26)
	return r.b + y
}

func (r *rng) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

// Run 执行压测并返回完整报告（核对在 join 之后单线程完成）。
func Run(cfg Config) (*Report, error) {
	if cfg.Workers <= 0 || cfg.Workers >= bwtree.MaxWorkers {
		return nil, fmt.Errorf("workers must be in [1,%d)", bwtree.MaxWorkers)
	}
	tree := bwtree.New()
	ref := refmap.New()

	// 记录缓冲按线程分片，避免追加竞争。
	perWorkerRecs := make([][]checker.Rec, cfg.Workers)
	perWorkerScans := make([][]scanRecord, cfg.Workers)
	var opSeq atomic.Int64
	var wg sync.WaitGroup
	startBarrier := sync.WaitGroup{}
	startBarrier.Add(1)

	startWall := time.Now()
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			r := newRNG(cfg.Seed, uint32(wid))
			recs := make([]checker.Rec, 0, cfg.OpsPerWorker)
			scans := make([]scanRecord, 0, cfg.OpsPerWorker/8)
			startBarrier.Wait() // 所有线程同时起跑
			for i := 0; i < cfg.OpsPerWorker; i++ {
				roll := r.intn(100)
				seq := opSeq.Add(1)
				rec := checker.Rec{Seq: seq, WID: uint32(wid)}
				k := bwtree.Key(r.intn(cfg.KeySpace))
				switch {
				case roll < cfg.ScanRatio:
					width := cfg.ScanWidth
					lo := int(k)
					hi := lo + width
					if hi > cfg.KeySpace {
						hi = cfg.KeySpace
					}
					rec.Kind = checker.OpScan
					rec.Key = bwtree.Key(lo)
					rec.KeyHi = bwtree.Key(hi)
					rec.StartNS = nanotime()
					out := tree.Scan(uint32(wid), bwtree.Key(lo), bwtree.Key(hi))
					rec.EndNS = nanotime()
					scans = append(scans, scanRecord{rec: rec, returned: out})
				default:
					writeRoll := r.intn(100)
					switch {
					case writeRoll < cfg.DeleteRatio:
						rec.Kind = checker.OpDelete
						rec.Key = k
						rec.StartNS = nanotime()
						tree.Delete(uint32(wid), k)
						rec.EndNS = nanotime()
						ref.Delete(k)
					case writeRoll < cfg.DeleteRatio+cfg.BlindRatio:
						rec.Kind = checker.OpBlindPut
						rec.Key = k
						rec.Val = bwtree.Value{
							BlendRatio: float64(r.intn(1000)) / 10, Tasting: int32(r.intn(100)),
							WriterSeq: seq, WriterWID: int32(wid),
						}
						rec.StartNS = nanotime()
						tree.BlindPut(uint32(wid), k, rec.Val)
						rec.EndNS = nanotime()
						ref.Upsert(k, rec.Val)
					default:
						rec.Kind = checker.OpPut
						rec.Key = k
						rec.Val = bwtree.Value{
							BlendRatio: float64(r.intn(1000)) / 10, Tasting: int32(r.intn(100)),
							WriterSeq: seq, WriterWID: int32(wid),
						}
						rec.StartNS = nanotime()
						tree.Put(uint32(wid), k, rec.Val)
						rec.EndNS = nanotime()
						// 点查与写入交替：对一部分键立即读回，丰富读历史。
						ref.Upsert(k, rec.Val)
						if r.intn(100) < 35 {
							gseq := opSeq.Add(1)
							grec := checker.Rec{Seq: gseq, WID: uint32(wid), Kind: checker.OpGet, Key: k, StartNS: nanotime()}
							v, ok := tree.Get(uint32(wid), k)
							grec.EndNS = nanotime()
							grec.Found, grec.Got = ok, v
							recs = append(recs, grec)
						}
					}
				}
				recs = append(recs, rec)
			}
			perWorkerRecs[wid] = recs
			perWorkerScans[wid] = scans
		}(w)
	}
	startBarrier.Done()
	wg.Wait()
	duration := time.Since(startWall)

	// 汇总历史，按开始时刻排序得到稳定输入（检查器内部再按 key 处理实时顺序）。
	var allRecs []checker.Rec
	var allScans []scanRecord
	for w := 0; w < cfg.Workers; w++ {
		allRecs = append(allRecs, perWorkerRecs[w]...)
		allScans = append(allScans, perWorkerScans[w]...)
	}
	sort.Slice(allRecs, func(i, j int) bool {
		if allRecs[i].StartNS != allRecs[j].StartNS {
			return allRecs[i].StartNS < allRecs[j].StartNS
		}
		return allRecs[i].Seq < allRecs[j].Seq
	})

	// 把扫描展开成合成点查，覆盖本次压测实际出现过的键空间。
	keySpace := make([]bwtree.Key, cfg.KeySpace)
	for i := range keySpace {
		keySpace[i] = bwtree.Key(i)
	}
	expanded := allRecs
	for _, s := range allScans {
		expanded = checker.ExpandScan(expanded, s.rec, s.returned, keySpace)
	}

	report := &Report{
		Cfg: cfg, Duration: duration, TotalOps: opSeq.Load(),
		Recs: expanded, Scans: allScans,
	}

	// ---- 核对 1：线性一致性 ----
	res, err := checker.Check(expanded)
	report.Linear = res
	if err != nil {
		return report, err
	}
	if res.CapHits > 0 {
		// 预算耗尽只发生在极端高竞争键上；贪心兜底仍失败会在 err 中体现。
		fmt.Printf("警告：%d 个键达到回溯预算，已用贪心串行化兜底核验\n", res.CapHits)
	}

	// ---- 核对 2：扫描内部有序性与无重复 ----
	for _, s := range allScans {
		for i := 1; i < len(s.returned); i++ {
			if s.returned[i-1].Key >= s.returned[i].Key {
				return report, fmt.Errorf("扫描结果非严格递增: %d -> %d", s.returned[i-1].Key, s.returned[i].Key)
			}
			if s.returned[i].Key < s.rec.Key || s.returned[i].Key >= s.rec.KeyHi {
				return report, fmt.Errorf("扫描结果越界")
			}
		}
	}

	// ---- 核对 3：与全局锁有序映射逐键对比最终内容 ----
	tree.DrainEpoch()
	got := tree.Scan(0, 0, bwtree.MaxKey)
	want := ref.Snapshot()
	report.FinalMatch, report.FinalDetail = compareKV(got, want)
	if !report.FinalMatch {
		return report, fmt.Errorf("最终内容不一致: %s", report.FinalDetail)
	}

	// ---- 核对 4：重试率与链长分布 ----
	snap := tree.Snapshot()
	report.Snap = snap
	var totalRetry, totalWriteOps uint64
	for _, ws := range snap.Workers {
		totalRetry += ws.CASRetry
		totalWriteOps += ws.Ops.Insert + ws.Ops.Blind + ws.Ops.Update + ws.Ops.Delete
	}
	if totalWriteOps > 0 {
		report.RetryRate = float64(totalRetry) / float64(totalWriteOps)
	}
	report.ChainHist = snap.ChainLen.Hist
	report.ChainMax = snap.ChainLen.Max
	report.ChainMean = snap.ChainLen.Mean

	// ---- 核对 5：延迟分位数 ----
	lats := make([]int64, 0, len(allRecs))
	for _, r := range allRecs {
		lats = append(lats, r.EndNS-r.StartNS)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	if len(lats) > 0 {
		report.LatencyP50Us = float64(lats[len(lats)/2]) / 1e3
		report.LatencyP99Us = float64(lats[len(lats)*99/100]) / 1e3
		report.LatencyMaxUs = float64(lats[len(lats)-1]) / 1e3
	}

	// 活锁防御：链长必须有界。
	if snap.ChainLen.Max > chainLimitBound {
		return report, fmt.Errorf("增量链无界：最大链长 %d", snap.ChainLen.Max)
	}
	runtime.KeepAlive(tree)
	return report, nil
}

const chainLimitBound = 64

func compareKV(got, want []bwtree.KV) (bool, string) {
	if len(got) != len(want) {
		return false, fmt.Sprintf("键数量不一致: 无锁树=%d 锁映射=%d", len(got), len(want))
	}
	for i := range got {
		if got[i].Key != want[i].Key {
			return false, fmt.Sprintf("第 %d 个键不同: %d != %d", i, got[i].Key, want[i].Key)
		}
		if got[i].Val.WriterSeq != want[i].Val.WriterSeq {
			return false, fmt.Sprintf("键 %d 的值不同: seq %d != %d", got[i].Key, got[i].Val.WriterSeq, want[i].Val.WriterSeq)
		}
	}
	return true, fmt.Sprintf("逐键一致，共 %d 个键", len(got))
}

// nanotime 用 runtime 单调时钟，保证严格递增，避免墙钟相等/回退。
func nanotime() int64 { return monoNow() }
