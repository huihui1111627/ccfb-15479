// Package checker 对压测记录做线性一致性（linearizability）核验。
//
// 每个操作都记录了开始/结束时刻；合法的线性化要求存在一个全序，使得：
//  1. 操作在全序中的先后不违反实时顺序（A 先结束于 B 开始之前，则 A 必须在 B 前）；
//  2. 每个读操作返回的值等于它之前最近一次写操作写入的值。
//
// 对同一个键而言这是原子读/写寄存器问题，按键独立做带回溯的 DFS，
// 用“可调度集合 + 最近写”记忆化剪枝；范围扫描展开成对覆盖键的合成点查。
package checker

import (
	"fmt"
	"sort"

	"bwtea/pkg/bwtree"
)

// OpKind 是操作类别。
type OpKind uint8

const (
	OpPut      OpKind = 1 // 插入或更新（写）
	OpBlindPut OpKind = 2 // 盲写（写）
	OpDelete   OpKind = 3 // 删除（写，值为墓碑）
	OpGet      OpKind = 4 // 点查（读）
	OpScan     OpKind = 5 // 范围扫描（展开为合成读）
)

// Rec 是一个操作的完整记录。
type Rec struct {
	Seq     int64        // 全局操作序号
	WID     uint32       // 执行线程
	Kind    OpKind       // 操作类别
	Key     bwtree.Key   // 点操作的键；扫描为下界
	KeyHi   bwtree.Key   // 扫描上界（排他）
	Val     bwtree.Value // 写入值
	Found   bool         // 读结果是否命中
	Got     bwtree.Value // 读返回值
	StartNS int64        // 开始时刻（纳秒）
	EndNS   int64        // 结束时刻（纳秒）
}

// tombstone 用 WriterSeq 的特殊值表示删除后的墓碑状态。
const tombstoneSeq int64 = -1

type keyOp struct {
	idx      int
	rec      *Rec
	isWrite  bool
	writeSeq int64 // 写值标识；删除为 tombstoneSeq
	found    bool
	gotSeq   int64
}

type memoKey struct {
	mask      uint64 // 历史不超过 64 个操作时的精确调度位掩码；否则为 0 表示不记忆化
	lastSeq   int64
	lastIsDel bool
	useMemo   bool
}

// Result 是核验结果。
type Result struct {
	KeysChecked  int
	OpsChecked   int
	Backtracks   int
	CapHits      int // 达到回溯预算、改用贪心兜底的键数
	FailedKey    bwtree.Key
	FailureReason string
}

// maxBacktracks 是单键 DFS 的回溯预算。
const maxBacktracks = 200_000

// Check 对全部操作记录做线性一致性核验。
func Check(recs []Rec) (Result, error) {
	byKey := make(map[bwtree.Key][]keyOp)
	// 扫描展开：对范围内的每个键生成一个合成读，时刻取扫描整体窗口。
	for i := range recs {
		r := &recs[i]
		switch r.Kind {
		case OpPut, OpBlindPut:
			byKey[r.Key] = append(byKey[r.Key], keyOp{
				idx: i, rec: r, isWrite: true, writeSeq: r.Val.WriterSeq,
			})
		case OpDelete:
			byKey[r.Key] = append(byKey[r.Key], keyOp{
				idx: i, rec: r, isWrite: true, writeSeq: tombstoneSeq,
			})
		case OpGet:
			gotSeq := tombstoneSeq
			if r.Found {
				gotSeq = r.Got.WriterSeq
			}
			byKey[r.Key] = append(byKey[r.Key], keyOp{
				idx: i, rec: r, isWrite: false, found: r.Found, gotSeq: gotSeq,
			})
		case OpScan:
			// 扫描结果在 Stress 层已逐键核对过范围顺序，这里把它当作对覆盖键的读。
			// 具体覆盖键集合由调用方通过 ExpandScan 预先注入，见下方处理。
		}
	}
	// 扫描展开由 ExpandScan 完成（需要扫描结果明细，在驱动里注入合成 Rec）。

	keys := make([]bwtree.Key, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	res := Result{}
	for _, k := range keys {
		ops := byKey[k]
		sort.SliceStable(ops, func(i, j int) bool {
			if ops[i].rec.StartNS != ops[j].rec.StartNS {
				return ops[i].rec.StartNS < ops[j].rec.StartNS
			}
			return ops[i].idx < ops[j].idx
		})
		ok, backtracks, capped := linearizeKey(k, ops)
		res.Backtracks += backtracks
		if capped {
			res.CapHits++
		}
		res.KeysChecked++
		res.OpsChecked += len(ops)
		if !ok {
			res.FailedKey = k
			res.FailureReason = fmt.Sprintf("键 %d 的操作历史无法排成合法串行顺序（回溯 %d 次）", k, backtracks)
			return res, fmt.Errorf("linearizability check failed at key %d", k)
		}
	}
	return res, nil
}

// ExpandScan 把一次扫描按其返回明细展开成合成点查记录，追加到 recs 并返回新切片。
func ExpandScan(recs []Rec, scan Rec, returned []bwtree.KV, keySpace []bwtree.Key) []Rec {
	got := make(map[bwtree.Key]bwtree.Value, len(returned))
	hit := make(map[bwtree.Key]bool, len(returned))
	for _, kv := range returned {
		got[kv.Key] = kv.Val
		hit[kv.Key] = true
	}
	for _, k := range keySpace {
		if k < scan.Key || k >= scan.KeyHi {
			continue
		}
		syn := Rec{
			Seq:     scan.Seq,
			WID:     scan.WID,
			Kind:    OpGet,
			Key:     k,
			StartNS: scan.StartNS,
			EndNS:   scan.EndNS,
			Found:   hit[k],
		}
		if hit[k] {
			syn.Got = got[k]
		}
		recs = append(recs, syn)
	}
	return recs
}

// linearizeKey 在单个键的操作历史上做 DFS 线性化。
func linearizeKey(k bwtree.Key, ops []keyOp) (bool, int, bool) {
	n := len(ops)
	memo := make(map[memoKey]bool, 256)
	backtracks := 0
	capped := false

	// available 返回在给定已调度集合下，满足实时顺序可立即调度的操作下标。
	available := func(scheduled []bool) []int {
		out := make([]int, 0, 4)
		for i, op := range ops {
			if scheduled[i] {
				continue
			}
			ok := true
			for j, other := range ops {
				if i == j || scheduled[j] {
					continue
				}
				// other 严格先于 op（other 已结束且 op 才开始）时，op 还不能调度。
				// 用严格 <：时间戳相等意味着两操作并发，任一线性序都合法，不强制先后。
				if other.rec.EndNS < op.rec.StartNS {
					ok = false
					break
				}
			}
			if ok {
				out = append(out, i)
			}
		}
		return out
	}

	var dfs func(scheduled []bool, lastSeq int64, lastDel bool, doneCount int) bool
	dfs = func(scheduled []bool, lastSeq int64, lastDel bool, doneCount int) bool {
		if backtracks > maxBacktracks {
			capped = true
			return greedyFallback(ops, lastSeq, lastDel)
		}
		if doneCount == n {
			return true
		}
		// 历史不超过 64 个操作时用精确位掩码记忆化；更大的历史不记忆化，保证正确性。
		var mk memoKey
		useMemo := n <= 64
		if useMemo {
			var mask uint64
			for i, s := range scheduled {
				if s {
					mask |= 1 << uint(i)
				}
			}
			mk = memoKey{mask: mask, lastSeq: lastSeq, lastIsDel: lastDel, useMemo: true}
			if v, seen := memo[mk]; seen {
				return v
			}
		}
		avail := available(scheduled)
		// 读优先：能直接满足的读先消费，减少分支。
		for _, i := range avail {
			op := ops[i]
			if op.isWrite {
				continue
			}
			want := op.gotSeq
			got := lastSeq
			if lastDel {
				got = tombstoneSeq
			}
			if want != got {
				continue // 该读此刻不合法，试试别的调度
			}
			scheduled[i] = true
			if dfs(scheduled, lastSeq, lastDel, doneCount+1) {
				if useMemo {
					memo[mk] = true
				}
				return true
			}
			scheduled[i] = false
			backtracks++
		}
		for _, i := range avail {
			op := ops[i]
			if !op.isWrite {
				continue
			}
			scheduled[i] = true
			isDel := op.writeSeq == tombstoneSeq
			if dfs(scheduled, op.writeSeq, isDel, doneCount+1) {
				if useMemo {
					memo[mk] = true
				}
				return true
			}
			scheduled[i] = false
			backtracks++
		}
		if useMemo {
			memo[mk] = false
		}
		return false
	}

	scheduled := make([]bool, n)
	ok := dfs(scheduled, tombstoneSeq, true, 0)
	return ok, backtracks, capped
}

// greedyFallback 在回溯预算耗尽时做一次贪心核验：
// 严格按结束时刻串行化，读必须匹配最近写；任何违例即判失败。
func greedyFallback(ops []keyOp, _ int64, _ bool) bool {
	sorted := make([]keyOp, len(ops))
	copy(sorted, ops)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].rec.EndNS != sorted[j].rec.EndNS {
			return sorted[i].rec.EndNS < sorted[j].rec.EndNS
		}
		return sorted[i].idx < sorted[j].idx
	})
	lastSeq := tombstoneSeq
	lastDel := true
	for _, op := range sorted {
		if op.isWrite {
			lastSeq = op.writeSeq
			lastDel = op.writeSeq == tombstoneSeq
			continue
		}
		got := lastSeq
		if lastDel {
			got = tombstoneSeq
		}
		if op.gotSeq != got {
			return false
		}
	}
	return true
}
