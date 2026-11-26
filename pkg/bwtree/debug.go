package bwtree

import (
	"encoding/json"
	"html/template"
	"net/http"
	"sort"
)

// Snapshot 采集索引内部结构的一致性快照（仅用于观测，不影响数据路径）。
func (t *Tree) Snapshot() Snapshot {
	snap := Snapshot{TakenAt: now(), RootPID: PID(t.rootPID.Load())}
	snap.SegmentTotal, snap.SegmentReturned, snap.SegmentReused = t.mapping.segmentStats()
	snap.DeltaKindCounts = make(map[string]uint64, kindCount)
	for k := uint8(1); k < kindCount; k++ {
		if v := t.deltaCounters.c[k].Load(); v != 0 {
			snap.DeltaKindCounts[deltaKindName[k]] = v
		}
	}

	t.liveMu.RLock()
	pids := make([]PID, 0, len(t.livePIDs))
	for p := range t.livePIDs {
		pids = append(pids, p)
	}
	t.liveMu.RUnlock()
	sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })

	buckets := make([]int, 6)
	totalLen := 0
	for _, p := range pids {
		head := t.mapping.load(p)
		if head == nil {
			continue
		}
		v := walkChain(head)
		snap.LiveNodes++
		snap.Nodes = append(snap.Nodes, NodeChainStat{
			PID: p, Kind: v.base.kind, Lo: v.lo, Hi: v.hi, Len: v.chainLen,
		})
		totalLen += v.chainLen
		if v.chainLen > snap.ChainLen.Max {
			snap.ChainLen.Max = v.chainLen
		}
		switch {
		case v.chainLen <= 1:
			buckets[0]++
		case v.chainLen == 2:
			buckets[1]++
		case v.chainLen <= 4:
			buckets[2]++
		case v.chainLen <= 8:
			buckets[3]++
		case v.chainLen <= 16:
			buckets[4]++
		case v.chainLen <= 32:
			buckets[5]++
		default:
			// 33+ 也计入最后一桶之外不新增，保持 6 桶，超长者单独靠 Max 体现。
			buckets[5]++
		}
	}
	snap.ChainLen.Hist = buckets
	if snap.LiveNodes > 0 {
		snap.ChainLen.Mean = float64(totalLen) / float64(snap.LiveNodes)
	}

	for w := 0; w < MaxWorkers; w++ {
		st := &t.stats[w]
		used := st.opsInsert+st.opsBlind+st.opsUpdate+st.opsDelete+st.opsGet+st.opsScan != 0
		if !used {
			continue
		}
		ws := WorkerStat{
			WID: uint32(w), CASRetry: st.casRetry, SplitHelp: st.splitHelp,
			MergeAbort: st.mergeAbort, Consolidate: st.consolidate,
			ScanRestart: st.scanRestart, LocRestart: st.locRestart,
			EpochAdvance: st.epochAdvance,
		}
		ws.Ops.Insert = st.opsInsert
		ws.Ops.Blind = st.opsBlind
		ws.Ops.Update = st.opsUpdate
		ws.Ops.Delete = st.opsDelete
		ws.Ops.Get = st.opsGet
		ws.Ops.Scan = st.opsScan
		snap.Workers = append(snap.Workers, ws)

		freeLen, alloc, reuse := t.pools[w].stats()
		snap.Pools = append(snap.Pools, PoolStat{WID: uint32(w), FreeLen: freeLen, Alloc: alloc, Reuse: reuse})
	}

	// 时间线环形缓冲：按写入序号还原最近事件。
	base := t.eventSeq.Load()
	start := uint64(0)
	if base > timelineCap {
		start = base - timelineCap
	}
	for seq := start + 1; seq <= base; seq++ {
		ev := t.events[(seq-1)%timelineCap]
		if ev.Seq == seq {
			snap.Timeline = append(snap.Timeline, ev)
		}
	}
	return snap
}

// ServeHTTP 提供 /debug/bwtree（页面）与 /debug/bwtree/json（数据）。
func (t *Tree) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	snap := t.Snapshot()
	if r.URL.Path == "/debug/bwtree/json" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(snap)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = debugTmpl.Execute(w, snap)
}

const debugHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>无锁有序索引 · 内部结构</title>
<meta http-equiv="refresh" content="2">
<style>
body{font-family:-apple-system,"PingFang SC",sans-serif;margin:24px;color:#1f2933;background:#fafafa}
h1{font-size:20px} h2{font-size:16px;margin-top:28px;border-left:4px solid #2b6cb0;padding-left:8px}
table{border-collapse:collapse;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.06)}
th,td{border:1px solid #e2e8f0;padding:6px 10px;font-size:13px;text-align:right}
th:first-child,td:first-child{text-align:left}
th{background:#edf2f7}
.kv{display:flex;gap:24px;flex-wrap:wrap;font-size:13px}
.kv div{background:#fff;padding:10px 16px;border-radius:6px;box-shadow:0 1px 2px rgba(0,0,0,.06)}
.kv b{font-size:18px;color:#2b6cb0}
.small{max-height:420px;overflow:auto}
</style>
</head>
<body>
<h1>无锁有序索引（Bw-tree 风格）内部结构 · 快照时刻 {{.TakenAt.Format "15:04:05.000"}}</h1>
<div class="kv">
  <div>根逻辑号 <b>{{.RootPID}}</b></div>
  <div>存活节点 <b>{{.LiveNodes}}</b></div>
  <div>映射表段数 <b>{{.SegmentTotal}}</b></div>
  <div>整段归还 <b>{{.SegmentReturned}}</b></div>
  <div>整段复用 <b>{{.SegmentReused}}</b></div>
  <div>最大链长 <b>{{.ChainLen.Max}}</b></div>
  <div>平均链长 <b>{{printf "%.2f" .ChainLen.Mean}}</b></div>
</div>

<h2>① 映射表：每个逻辑节点的增量链长度</h2>
<table class="small">
<tr><th>逻辑号 PID</th><th>节点类型</th><th>键范围下界</th><th>键范围上界</th><th>增量链长度</th></tr>
{{range .Nodes}}
<tr><td>{{.PID}}</td><td>{{if eq .Kind 1}}叶子{{else}}内部{{end}}</td><td>{{.Lo}}</td><td>{{.Hi}}</td><td>{{.Len}}</td></tr>
{{end}}
</table>
<p style="font-size:12px;color:#666">链长分桶（1 / 2 / 3-4 / 5-8 / 9-16 / 17+）：
{{range .ChainLen.Hist}}[{{.}}] {{end}}</p>

<h2>② 各类增量记录数量</h2>
<table>
<tr><th>记录类型</th><th>当前存量</th></tr>
{{range $k, $v := .DeltaKindCounts}}
<tr><td>{{$k}}</td><td>{{$v}}</td></tr>
{{end}}
</table>

<h2>③ 分裂 / 合并时间线（最近 {{len .Timeline}} 条）</h2>
<table class="small">
<tr><th>序号</th><th>时刻</th><th>事件</th><th>线程</th><th>节点</th><th>兄弟</th><th>父</th><th>分裂/分隔键</th><th>说明</th></tr>
{{range .Timeline}}
<tr><td>{{.Seq}}</td><td>{{.At.Format "15:04:05.000"}}</td><td>{{.Kind}}</td><td>{{.WID}}</td>
<td>{{.Node}}</td><td>{{.Sibling}}</td><td>{{.Parent}}</td><td>{{.SplitKey}}</td><td style="text-align:left">{{.Detail}}</td></tr>
{{end}}
</table>

<h2>④ 各线程冲突重试与内存池</h2>
<table class="small">
<tr><th>线程</th><th>CAS重试</th><th>协助分裂</th><th>放弃合并</th><th>物化次数</th>
<th>扫描重启</th><th>重定位</th><th>epoch推进</th>
<th>插入</th><th>盲写</th><th>更新</th><th>删除</th><th>点查</th><th>扫描</th>
<th>池空闲</th><th>池新分配</th><th>池重用</th></tr>
{{$pools := .Pools}}
{{range .Workers}}
<tr><td>{{.WID}}</td><td>{{.CASRetry}}</td><td>{{.SplitHelp}}</td><td>{{.MergeAbort}}</td>
<td>{{.Consolidate}}</td><td>{{.ScanRestart}}</td><td>{{.LocRestart}}</td><td>{{.EpochAdvance}}</td>
<td>{{.Ops.Insert}}</td><td>{{.Ops.Blind}}</td><td>{{.Ops.Update}}</td><td>{{.Ops.Delete}}</td>
<td>{{.Ops.Get}}</td><td>{{.Ops.Scan}}</td>
<td>{{poolFree .WID $pools}}</td><td>{{poolAlloc .WID $pools}}</td><td>{{poolReuse .WID $pools}}</td></tr>
{{end}}
</table>
</body></html>`

var debugTmpl = template.Must(template.New("debug").Funcs(template.FuncMap{
	"poolFree": func(wid uint32, pools []PoolStat) uint64 {
		for _, p := range pools {
			if p.WID == wid {
				return p.FreeLen
			}
		}
		return 0
	},
	"poolAlloc": func(wid uint32, pools []PoolStat) uint64 {
		for _, p := range pools {
			if p.WID == wid {
				return p.Alloc
			}
		}
		return 0
	},
	"poolReuse": func(wid uint32, pools []PoolStat) uint64 {
		for _, p := range pools {
			if p.WID == wid {
				return p.Reuse
			}
		}
		return 0
	},
}).Parse(debugHTML))
