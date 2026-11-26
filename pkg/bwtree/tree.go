package bwtree

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// 物化与分裂/合并阈值（以叶子条目数计）。
const (
	leafSplitAt = 128 // 物化后条目数达到该值即分裂
	leafMergeAt = 16 // 物化后条目数低于该值且存在左兄弟则尝试保守合并（不立即回收右节点）
	chainLimit    = 24 // 增量链长度达到该值即触发行人所在线程的物化
	maxLoops      = 1_000_000
	relocateBound = 256 // 单次定位内的重定位预算，超过即走强制兜底保证终止
)

// latchFor 返回某叶子 PID 对应的写门闩。每个 PID 一把独立锁，杜绝分段哈希冲突；
	// 锁对象按需创建并缓存在 latchMu 保护的 map 中。
func (t *Tree) latchFor(pid PID) *sync.Mutex {
	t.latchMu.Lock()
	defer t.latchMu.Unlock()
	if m, ok := t.leafLatches[pid]; ok {
		return m
	}
	m := &sync.Mutex{}
	t.leafLatches[pid] = m
	return m
}

// ErrNotFound 表示键不存在。
var ErrNotFound = errors.New("bwtree: key not found")

// Tree 是无锁有序索引。
type Tree struct {
	mapping *mappingTable
	epoch   *epochMgr
	rootPID atomic.Uint64

	pools [MaxWorkers]*recPool
	stats [MaxWorkers]workerStats

	// 分裂/合并时间线：原子序号 + 环形缓冲（固定容量，无锁写入槽位）。
	eventSeq atomic.Uint64
	events   [timelineCap]TimelineEvent

	deltaCounters deltaCounters

	// 叶子写门闩：每个 PID 一把独立锁，串行化同一叶子的数据写与该叶子的分裂/物化，
	// 从而关闭分裂窗口内“写与结构变迁交叉”导致的写错节点竞态。读路径不加锁，仍为无锁快照读。
	latchMu     sync.Mutex
	leafLatches map[PID]*sync.Mutex

	nodeMu   sync.Mutex // 仅保护“活跃逻辑节点名单”的快照维护，不在数据路径加锁
	liveMu   sync.RWMutex
	livePIDs map[PID]struct{}
}

const timelineCap = 2048

// New 创建一个空索引。
func New() *Tree {
	t := &Tree{livePIDs: make(map[PID]struct{})}
	t.mapping = newMappingTable()
	t.epoch = newEpochMgr(t.reclaim)
	t.leafLatches = make(map[PID]*sync.Mutex)
	for i := range t.pools {
		t.pools[i] = newRecPool()
	}
	// 根初始化为一个空叶子，负责全键范围 [0, MaxKey)。
	root := &Record{
		kind:  recLeafBase,
		lo:    0,
		hi:    MaxKey,
		lp:    &leafPayload{},
		owner: MaxWorkers, // 基础设施记录不属于任何工作线程，回收时直接丢弃
	}
	pid := t.mapping.allocPID(root)
	t.rootPID.Store(uint64(pid))
	t.markLive(pid)
	t.countRecord(root) // 初始根基础记录也纳入类型计数
	return t
}

// ---- epoch 临界区包装 ----

func (t *Tree) enter(wid uint32) uint64 { return t.epoch.enter(wid) }
func (t *Tree) leave(wid uint32)        { t.epoch.leave(wid) }

// reclaim 是 epoch 到期后的回收回调：记录退回原属池，废弃 PID 释放。
// reclaim 是 epoch 到期后的回收回调。
// 关键安全约束：发布过（曾挂到某条增量链/映射表）的记录绝不退回线程池重用。
// 因为别的线程可能仍通过旧快照持有它的指针，重用同一地址会形成跨链环。
// Go 有 GC，这里只需解除索引侧引用并记账，内存由 GC 回收；PID 则可安全复用。
// 线程池的物理重用只服务于“CAS 失败、从未发布”的增量（见 releaseRecord）。
func (t *Tree) reclaim(item limboItem) {
	for _, r := range item.records {
		if r.owner < MaxWorkers {
			t.pools[r.owner].noteReclaimed() // 仅记账，不物理重用
		}
	}
	if item.freePID != invalidPID {
		t.mapping.freePID(item.freePID)
		t.unmarkLive(item.freePID)
	}
}

func (t *Tree) markLive(pid PID) {
	t.liveMu.Lock()
	t.livePIDs[pid] = struct{}{}
	t.liveMu.Unlock()
}

func (t *Tree) unmarkLive(pid PID) {
	t.liveMu.Lock()
	delete(t.livePIDs, pid)
	t.liveMu.Unlock()
}

// ---- 时间线 ----

func (t *Tree) logEvent(ev TimelineEvent) {
	seq := t.eventSeq.Add(1)
	ev.Seq = seq
	if ev.At.IsZero() {
		ev.At = time.Now()
	}
	t.events[(seq-1)%timelineCap] = ev
}

// ---- 增量链遍历视图 ----

// chainView 是一次链遍历得到的视图。
type chainView struct {
	head      *Record
	base      *Record // 链上的基础记录（链尾）
	lo, hi    Key     // 节点当前负责范围（结构增量会收窄）
	split     *Record // 未消费的分裂增量（nil 表示无）
	merge     *Record // 未消费的合并增量（nil 表示无）
	abort     *Record // 未消费的放弃合并增量
	chainLen  int
}

// walkChain 从物理头记录沿 next 走完整条增量链，汇总结构状态与范围。
// 从最新往最旧走：遇到“放弃合并”增量后，它所抵消的那条（更旧的）合并增量即视为失效。
func walkChain(head *Record) chainView {
	v := chainView{head: head}
	abortPending := 0
	seen := make(map[*Record]bool, 16)
	for r := head; r != nil; r = r.next {
		if seen[r] {
			// 环链防御：正常的增量链必须是无环单链表。出现环说明有记录被提前回收重用，
			// 在这里截断，避免无限循环；该节点的结构问题由上层重定位/兜底处理。
			break
		}
		seen[r] = true
		v.chainLen++
		switch r.kind {
		case recLeafBase, recInternalBase:
			v.base = r
			// 基础记录只在没有结构增量收窄范围时提供默认范围；
			// 分裂增量已经把 hi 收窄到分裂点，不能被旧基础记录覆盖。
			if v.split == nil {
				v.lo, v.hi = r.lo, r.hi
			} else {
				v.lo = r.lo
			}
		case recSplit:
			if v.split == nil {
				v.split = r
				v.hi = r.hi // 分裂后本节点只负责到分裂点
			}
		case recAbortMerge:
			abortPending++
			if v.abort == nil {
				v.abort = r
			}
		case recMerge:
			if abortPending > 0 {
				abortPending-- // 被更新的放弃增量抵消
			} else if v.merge == nil {
				v.merge = r
			}
		}
	}
	return v
}

// leafLookup 在叶子增量链上点查。found=false 表示键不存在（可能被删除）。
// 从最新的增量往旧的走，第一条命中的记录决定结果。
func leafLookup(head *Record, k Key) (Value, bool) {
	for r := head; r != nil; r = r.next {
		switch r.kind {
		case recBlindWrite, recInsert, recUpdate:
			if r.key == k {
				return r.val, true
			}
		case recDelete:
			if r.key == k {
				return Value{}, false
			}
		case recLeafBase:
			return r.lp.get(k)
		}
	}
	return Value{}, false
}

	// 内层节点：路由项增量命中范围就直接跳转；删除键增量屏蔽对应孩子。
// 选路统一走 collectActiveChildren 的“最大 lo<=k”不变量，不依赖可能陈旧的 hi。
func routeOnce(head *Record, k Key) PID {
	return routeOnceExcept(head, k, nil)
}

// routeOnceExcept 在选路时额外跳过 exclude 中的孩子（调用方已知其失效）。
func routeOnceExcept(head *Record, k Key, exclude map[PID]bool) PID {
	children := collectActiveChildren(head, exclude)
	return pickChild(children, k)
}

// routeRightmostActive 同样基于活孩子集合选路，供越界重定位使用。
func routeRightmostActive(head *Record, k Key, exclude map[PID]bool) PID {
	children := collectActiveChildren(head, exclude)
	return pickChild(children, k)
}

// collectActiveChildren 汇总内层节点当前仍有效的孩子（增量优先于基础表）。
// 返回按 lo 升序、lo 唯一（重复保留更新的那条）的路由项。
func collectActiveChildren(head *Record, exclude map[PID]bool) []routeEntry {
	removed := make(map[PID]bool)
	latest := make(map[PID]routeEntry)
	order := make([]PID, 0)
	for r := head; r != nil; r = r.next {
		switch r.kind {
		case recRemoveKey:
			removed[r.child] = true
			delete(latest, r.child)
		case recIndexEntry:
			if removed[r.child] || exclude[r.child] {
				continue
			}
			if _, seen := latest[r.child]; !seen {
				order = append(order, r.child)
			}
			latest[r.child] = routeEntry{child: r.child, lo: r.lo, hi: r.hi}
		case recInternalBase:
			for _, e := range r.ip.entries {
				if removed[e.child] || exclude[e.child] {
					continue
				}
				if _, seen := latest[e.child]; !seen {
					order = append(order, e.child)
					latest[e.child] = e
				}
			}
		}
	}
	out := make([]routeEntry, 0, len(order))
	for _, c := range order {
		if e, ok := latest[c]; ok {
			out = append(out, e)
		}
	}
	// 插入排序（路由项通常很少），按 lo 去重保留先出现者并升序。
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].lo > out[j].lo; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// pickChild 按 B+树不变量选孩子：lo<=k 的最右者；k 比所有 lo 都小则取最左者。
func pickChild(children []routeEntry, k Key) PID {
	if len(children) == 0 {
		return invalidPID
	}
	best := children[0].child
	for _, e := range children {
		if e.lo <= k {
			best = e.child
		} else {
			break
		}
	}
	return best
}

// ---- 定位叶子 ----

// locateLeaf 从根出发定位负责 key 的叶子，返回其逻辑号与物理头记录。
// 沿途遇到未完成的分裂会顺手补做第二步；拿着旧指针走到不负责目标键的节点时，
// 沿兄弟指针或回父节点重新定位。
func (t *Tree) locateLeaf(wid uint32, k Key) (PID, *Record) {
	dead := make(map[PID]bool, 8) // 跨外层重启累计的失效节点
	relocations := 0               // 重定位次数，用于有界降级保证终止性
	for loop := 0; loop < maxLoops; loop++ {
		// 双保险：内层重定位计数或外层重启次数任一超预算都强制兜底，保证终止。
		if relocations > relocateBound || loop > relocateBound {
			return t.forceLocateLeaf(k)
		}
		rootPID := PID(t.rootPID.Load())
		pid := rootPID
		var parentPid PID
		for depth := 0; depth < 80; depth++ {
			head := t.mapping.load(pid)
			if head == nil || dead[pid] {
				// 节点已被合并回收或本次已判失效：回父节点，并把它加入黑名单，
				// 这样父节点重选路时不会再选到同一个死孩子。
				t.stats[wid].locRestart++
				relocations++
				dead[pid] = true
				if parentPid != invalidPID && !dead[parentPid] {
					pid, parentPid = parentPid, 0
					continue
				}
				break // 父也失效：回根重来
			}
			v := walkChain(head)
			if v.merge != nil {
				// 合并标记：本节点内容已/将并入左兄弟，直接跳过去。
				t.stats[wid].locRestart++
				relocations++
				dead[pid] = true
				pid = v.merge.sib
				continue
			}
			if v.base == nil {
				break // 防御性回根
			}
			if k < v.lo || k >= v.hi {
				// 旧指针：本节点当前范围不含目标键。
				t.stats[wid].locRestart++
				relocations++
				if v.base != nil && v.base.kind == recLeafBase {
					// 叶子越界：优先走分裂兄弟；否则直接回根，由根的 pickChild 权威重选，
					// 不依赖可能被污染的 dead 集合，避免误选到本叶子或已失效节点。
					if v.split != nil && k >= v.split.hi && !dead[v.split.sib] {
						pid = v.split.sib
						continue
					}
					break // 回根重新完整定位
				}
				// 内层节点范围不符才拉黑。
				dead[pid] = true
				if v.split != nil && k >= v.split.hi && !dead[v.split.sib] {
					pid = v.split.sib
					continue
				}
				if parentPid != invalidPID && !dead[parentPid] {
					pid, parentPid = parentPid, 0
					continue
				}
				break
			}
			if v.base.kind == recLeafBase {
				if v.split != nil {
					// 分裂第一步已挂。按键决定归属：右半键（>= 分裂点）归新兄弟，左半归本节点。
					// 先协助完成第二步（幂等），再返回正确叶子的当前 head。
					if k >= v.split.hi {
						t.helpSplit(wid, pid, v.split)
						sibHead := t.mapping.load(v.split.sib)
						if sibHead != nil {
							return v.split.sib, sibHead
						}
						continue // 兄弟暂不可达，重读后经新父路由选路
					}
					t.helpSplit(wid, pid, v.split)
					continue
				}
				return pid, head
			}
			// 内层节点
			if v.split != nil {
				t.helpSplit(wid, pid, v.split)
				continue
			}
			// 内层节点链过长时，路过的线程顺手物化，防止路由增量无界累积。
			// 只做物化（不在这里发起分裂），CAS 失败说明别的线程已改动，无害跳过。
			if v.merge == nil && v.chainLen >= chainLimit {
				_, _, _ = t.consolidateWithLatch(wid, pid, head)
				continue
			}
			next := routeOnceExcept(head, k, dead)
			// 选路严格遵循 B+树不变量：lo<=k 的最右活孩子负责该键，与可能滞后的 hi 无关。
			// 不再因 k>=叶子.hi 就改派最右叶子——那会让同一键在不同线程落到不同叶子。
			if next == invalidPID || dead[next] {
				break // 没有可选的活孩子：回根重来
			}
			parentPid = pid
			pid = next
		}
	}
	panic("bwtree: locateLeaf exceeded max loops")
}

// forceLocateLeaf 是终止性兜底，保证返回一个当前存活的叶子（或在根已失效时回退到根叶子）。
// 它不带任何回溯：从根沿活孩子确定性下降，遇合并跳左兄弟，遇 nil 回到根重试若干次。
func (t *Tree) forceLocateLeaf(k Key) (PID, *Record) {
	for attempt := 0; attempt < 8; attempt++ {
		rootPID := PID(t.rootPID.Load())
		pid := rootPID
		for depth := 0; depth < 64; depth++ {
			head := t.mapping.load(pid)
			if head == nil {
				break // 回到根重试
			}
			v := walkChain(head)
			if v.merge != nil {
				pid = v.merge.sib
				continue
			}
			if v.base == nil {
				break
			}
			if v.base.kind == recLeafBase {
				// 分裂挂起时按分裂点导航到正确兄弟，避免把右半键留在收窄前的左叶子。
				if v.split != nil && k >= v.split.hi {
					sibHead := t.mapping.load(v.split.sib)
					if sibHead != nil {
						return v.split.sib, sibHead
					}
					break // 兄弟暂不可达，回根重试
				}
				return pid, head
			}
			children := collectActiveChildren(head, nil)
			if len(children) == 0 {
				break
			}
			next := pickChild(children, k)
			if next == invalidPID || next == pid {
				next = children[len(children)-1].child
			}
			pid = next
		}
	}
	// 最终保险：根本身在单节点树时就是叶子。
	rootPID := PID(t.rootPID.Load())
	head := t.mapping.load(rootPID)
	if head != nil {
		return rootPID, head
	}
	panic("bwtree: forceLocateLeaf could not find any leaf")
}

// helpSplit 补做分裂第二步：在父节点上挂路由项增量。
// 幂等：路由项携带子节点范围，已存在等价项时直接视为完成。
func (t *Tree) helpSplit(wid uint32, leftPID PID, split *Record) {
	parentPID := split.parent
	rightPID := split.sib
	splitKey := split.hi
	rightHi := split.nextRangeHi()

	entry := t.allocRecord(wid, recIndexEntry)
	entry.child = rightPID
	entry.lo = splitKey
	entry.hi = rightHi

	for loop := 0; loop < 256; loop++ {
		if parentPID == PID(t.rootPID.Load()) && t.mapping.load(parentPID) == nil {
			// 父是旧根且已被根分裂替换：右路由已在新根中，可安全物化左节点。
			t.pools[wid].put(entry)
			t.consolidateSplitLeft(wid, leftPID, rightPID)
			return
		}
		parentHead := t.mapping.load(parentPID)
		if parentHead == nil {
			// 父暂时不可达：不物化左节点，保留 split 增量维持可导航，等下一个访问者重试。
			t.pools[wid].put(entry)
			return
		}
		pv := walkChain(parentHead)
		if pv.base == nil || pv.base.kind != recInternalBase {
			// 父尚非内部节点（结构未就绪）：保留 split 增量，不物化，避免导航空洞。
			t.pools[wid].put(entry)
			return
		}
		if internalHasRange(parentHead, rightPID, splitKey, rightHi) {
			t.pools[wid].put(entry)
			t.consolidateSplitLeft(wid, leftPID, rightPID) // 右路由确认存在，可安全消费 split 增量
			return
		}
		// 同步收窄左孩子在父视角下的范围上界。
		if !internalHasChild(parentHead, leftPID) {
			// 左孩子已不在父路由：路由状态不明，保留 split 增量交由专门的结构整理，
			// 不在这里物化，避免在右路由不可达时制造空洞。
			t.pools[wid].put(entry)
			return
		}
		entry.next = parentHead
		if t.mapping.cas(parentPID, parentHead, entry) {
			t.stats[wid].splitHelp++
			t.logEvent(TimelineEvent{
				Kind: "split-help", WID: wid, Node: leftPID, Sibling: rightPID,
				Parent: parentPID, SplitKey: splitKey,
				Detail: "补做分裂第二步：父节点挂路由项增量",
			})
			t.consolidateSplitLeft(wid, leftPID, rightPID)
			return
		}
		t.stats[wid].casRetry++
	}
	// 256 次 CAS 仍未装上路由：保留 split 增量维持可导航，不强行物化，等后续访问者继续协助。
	t.pools[wid].put(entry)
}

// installParentRoute 只负责把右兄弟路由项装进父节点，绝不触发左节点收窄。
// 供 splitLeaf 在已持有左叶子门闩时调用，避免与锁内的收窄重入。返回路由是否确认就绪。
func (t *Tree) installParentRoute(wid uint32, leftPID PID, split *Record, rightPID PID, splitKey Key) bool {
	parentPID := split.parent
	rightHi := split.nextRangeHi()
	entry := t.allocRecord(wid, recIndexEntry)
	entry.child = rightPID
	entry.lo = splitKey
	entry.hi = rightHi
	for loop := 0; loop < 256; loop++ {
		parentHead := t.mapping.load(parentPID)
		if parentHead == nil {
			t.pools[wid].put(entry)
			return false
		}
		pv := walkChain(parentHead)
		if pv.base == nil || pv.base.kind != recInternalBase {
			t.pools[wid].put(entry)
			return false
		}
		if internalHasRange(parentHead, rightPID, splitKey, rightHi) {
			t.pools[wid].put(entry)
			return true // 路由已存在
		}
		if !internalHasChild(parentHead, leftPID) {
			t.pools[wid].put(entry)
			return false
		}
		entry.next = parentHead
		if t.mapping.cas(parentPID, parentHead, entry) {
			t.stats[wid].splitHelp++
			t.logEvent(TimelineEvent{
				Kind: "split-help", WID: wid, Node: leftPID, Sibling: rightPID,
				Parent: parentPID, SplitKey: splitKey,
				Detail: "分裂第二步：父节点挂路由项增量",
			})
			return true
		}
		t.stats[wid].casRetry++
	}
	t.pools[wid].put(entry)
	return false
}

// consolidateSplitLeft 物化带 split 增量的左节点，把 split 增量消费掉，
// 防止读者反复看到同一个未完成分裂而活锁。CAS 失败说明别的线程已改动，下一轮自然收敛。
// 物化产生的右半 excluded 必须转发到分裂兄弟，否则会丢写。
func (t *Tree) consolidateSplitLeft(wid uint32, leftPID PID, siblingPID PID) {
	head := t.mapping.load(leftPID)
	if head == nil {
		return
	}
	v := walkChain(head)
	if v.split == nil {
		return
	}
	_, excluded, ok := t.consolidateWithLatch(wid, leftPID, head)
	if ok && len(excluded) > 0 && siblingPID != invalidPID {
		t.forwardDeltasToSiblingReliable(wid, excluded, siblingPID)
	}
}

// nextRangeHi 取分裂后右兄弟的范围上界（记录在分裂增量紧随的旧链范围里）。
func (r *Record) nextRangeHi() Key {
	if r.next != nil {
		v := walkChain(r.next)
		return v.hi
	}
	return MaxKey
}

func internalHasRange(head *Record, child PID, lo, hi Key) bool {
	for x := head; x != nil; x = x.next {
		if (x.kind == recIndexEntry || x.kind == recRemoveKey) && x.child == child && x.lo == lo && x.hi == hi {
			return true
		}
		if x.kind == recInternalBase {
			for _, e := range x.ip.entries {
				if e.child == child && e.lo == lo && e.hi == hi {
					return true
				}
			}
		}
	}
	return false
}

func internalHasChild(head *Record, child PID) bool {
	removed := false
	for x := head; x != nil; x = x.next {
		if x.kind == recRemoveKey && x.child == child {
			removed = true // 更新的删除增量优先：此孩子已被摘除
		}
		if x.kind == recIndexEntry && x.child == child && !removed {
			return true
		}
		if x.kind == recInternalBase {
			if removed {
				return false
			}
			for _, e := range x.ip.entries {
				if e.child == child {
					return true
				}
			}
		}
	}
	return false
}
