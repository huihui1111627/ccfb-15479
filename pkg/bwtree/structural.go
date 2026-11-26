package bwtree

import "sort"

// consolidate 把 pid 的整条增量链物化成一条新的基础记录。
// 叶子节点的物化需持有该叶子的写门闩（由调用方保证，或走 consolidateLocked 包装）。
// 返回 (新基础记录, CAS 是否成功)。成功后旧链进入 epoch limbo。
func (t *Tree) consolidate(wid uint32, pid PID, head *Record) (*Record, []*Record, bool) {
	view := walkChain(head)
	if view.merge != nil || view.base == nil {
		return nil, nil, false
	}
	var newBase *Record
	var excluded []*Record
	switch view.base.kind {
	case recLeafBase:
		newBase, excluded = materializeLeaf(head, view.lo, view.hi)
	case recInternalBase:
		newBase = materializeInternal(head, view.lo, view.hi)
	default:
		return nil, nil, false
	}
	newBase.owner = wid
	if !t.mapping.cas(pid, head, newBase) {
		// 失败的新基础记录直接退回本池；它不承载任何子结构，安全。
		t.releaseRecord(wid, newBase)
		t.stats[wid].casRetry++
		return nil, nil, false
	}
	t.stats[wid].consolidate++
	// 旧链上的记录类型计数随回收归零。
	for r := head; r != nil; r = r.next {
		t.decCount(r.kind)
	}
	t.epoch.retire(head, invalidPID)
	// 内部节点物化后，被删除路由增量摘除的合并空壳子节点已不可达，安全延迟回收其 PID。
	if newBase.kind == recInternalBase {
		t.reapMergedShells(wid, head)
	}
	return newBase, excluded, true
}

// consolidateWithLatch 供未持有门闩的入口（maintainAfterWrite、helpSplit、读路径物化）使用。
// 在写临界区之外调用，用阻塞 Lock 与数据写互斥；不会与写者重入（写者进入前已 Unlock）。
func (t *Tree) consolidateWithLatch(wid uint32, pid PID, head *Record) (*Record, []*Record, bool) {
	view := walkChain(head)
	if view.base != nil && view.base.kind == recLeafBase {
		latch := t.latchFor(pid)
		latch.Lock()
		defer latch.Unlock()
		cur := t.mapping.load(pid)
		if cur != head {
			head = cur // 等锁期间链头已变，用最新头重做
		}
	}
	return t.consolidate(wid, pid, head)
}

// reapMergedShells 回收内部节点旧链上已被 recRemoveKey 摘除、且自身带合并标记的空壳叶子。
// 调用前提：父节点已成功物化为不含这些孩子的新基础记录，旧父链也已进入 epoch 隔离，
// 因而没有新读者能再路由到这些空壳；回收其逻辑号可触发整段归还/复用。
func (t *Tree) reapMergedShells(wid uint32, oldParentHead *Record) {
	removed := make(map[PID]bool)
	for r := oldParentHead; r != nil; r = r.next {
		if r.kind == recRemoveKey {
			removed[r.child] = true
		}
	}
	for child := range removed {
		childHead := t.mapping.load(child)
		if childHead == nil {
			continue
		}
		cv := walkChain(childHead)
		if cv.merge == nil {
			continue // 仅回收确实处于合并空壳状态的节点
		}
		// 先把映射表项摘掉，再连同整条链交给 epoch，到期释放 PID。
		if t.mapping.cas(child, childHead, nil) {
			for r := childHead; r != nil; r = r.next {
				t.decCount(r.kind)
			}
			t.epoch.retire(childHead, child)
			t.unmarkLive(child)
		}
	}
}

// postConsolidate 物化后按节点大小决定分裂或合并。
func (t *Tree) postConsolidate(wid uint32, pid PID, base *Record) {
	switch base.kind {
	case recLeafBase:
		if len(base.lp.keys) >= leafSplitAt {
			t.splitLeaf(wid, pid, base)
			return
		}
		if len(base.lp.keys) < leafMergeAt {
			t.tryMergeLeaf(wid, pid, base)
		}
	case recInternalBase:
		if len(base.ip.entries) >= leafSplitAt {
			t.splitInternal(wid, pid, base)
		}
	}
}

// ---- 叶子分裂（两阶段）----

func (t *Tree) splitLeaf(wid uint32, leftPID PID, leftBase *Record) {
	for attempt := 0; attempt < 16; attempt++ {
		head := t.mapping.load(leftPID)
		view := walkChain(head)
		if view.merge != nil {
			// 规定的先后：合并标记先挂则分裂放弃。
			t.stats[wid].mergeAbort++
			t.logEvent(TimelineEvent{Kind: "merge-abort", WID: wid, Node: leftPID,
				Detail: "分裂遇已挂合并增量，按先后规则放弃分裂"})
			return
		}
		if view.split != nil {
			t.helpSplit(wid, leftPID, view.split)
			return
		}
		if view.base == nil || view.base.kind != recLeafBase {
			return
		}
		base := view.base
		n := len(base.lp.keys)
		if n < leafSplitAt {
			return // 别的线程已经拆过/删过
		}
		// 关键正确性：仅当链上除基础记录外没有任何待处理数据增量时才分裂。
		// 否则并发写挂在链上的数据增量可能在按分裂点裁剪时被丢弃（写丢失）。
		// 有增量时本次只物化，下一轮在纯基础记录上再分裂。
		if view.chainLen > 1 {
			if nb, _, ok := t.consolidate(wid, leftPID, head); ok {
				t.postConsolidate(wid, leftPID, nb)
			}
			return
		}
		mid := n / 2
		splitKey := base.lp.keys[mid]

		// 第一步：造右兄弟（新基础叶子），再在左节点挂分裂增量指向它。
		rightBase := t.allocRecord(wid, recLeafBase)
		rightBase.owner = wid
		rightBase.lo, rightBase.hi = splitKey, base.hi
		rightBase.lp = &leafPayload{
			keys: append([]Key(nil), base.lp.keys[mid:]...),
			vals: append([]Value(nil), base.lp.vals[mid:]...),
		}
		rightPID := t.mapping.allocPID(rightBase)
		t.markLive(rightPID)

		split := t.allocRecord(wid, recSplit)
		split.sib = rightPID
		split.hi = splitKey
		split.lo = base.lo
		split.next = head

		parentPID := t.findParent(wid, leftPID, splitKey)
		split.parent = parentPID
		// 整个分裂（挂 split → 装父路由 → 收窄左节点）在同一把左叶子门闩内完成，
		// 与该叶子的所有数据写完全互斥，从根本上消除分裂窗口的写错/丢写竞态。
		latch := t.latchFor(leftPID)
		latch.Lock()
		curHead := t.mapping.load(leftPID)
		curView := walkChain(curHead)
		if curHead != head || curView.split != nil || curView.merge != nil {
			latch.Unlock()
			t.releaseRecord(wid, split)
			t.mapping.freePID(rightPID)
			t.unmarkLive(rightPID)
			t.releaseRecord(wid, rightBase)
			t.stats[wid].casRetry++
			continue
		}
		if !t.mapping.cas(leftPID, curHead, split) {
			latch.Unlock()
			t.releaseRecord(wid, split)
			t.mapping.freePID(rightPID)
			t.unmarkLive(rightPID)
			t.releaseRecord(wid, rightBase)
			t.stats[wid].casRetry++
			continue
		}
		t.logEvent(TimelineEvent{Kind: "split", WID: wid, Node: leftPID, Sibling: rightPID,
			Parent: parentPID, SplitKey: splitKey, Detail: "叶子分裂第一步：挂分裂增量"})

		// 第二步：在父节点挂路由项（根分裂则安装新内部根），只装路由、不触发左节点收窄。
		routeReady := false
		if parentPID == leftPID {
			t.finishRootSplit(wid, leftPID, split, rightPID, splitKey)
			routeReady = true
		} else {
			routeReady = t.installParentRoute(wid, leftPID, split, rightPID, splitKey)
		}

		// 第三步：仍持锁。只有右路由确认就绪才收窄左节点；否则保留 split 维持导航，
		// 等后续访问者经 helpSplit 协助，绝不提前收窄制造导航空洞。
		if routeReady {
			leftHead := t.mapping.load(leftPID)
			lv := walkChain(leftHead)
			if lv.split != nil {
				newLeft, excluded, ok := t.consolidate(wid, leftPID, leftHead)
				if ok && len(excluded) > 0 {
					// 右半残留转发到右兄弟；锁序恒为左→右，无死锁环。
					t.forwardDeltasToSiblingReliable(wid, excluded, rightPID)
				}
				_ = newLeft
			}
		}
		latch.Unlock()
		return
	}
}

// forwardDeltasToSibling 把物化时被范围裁剪排除的数据增量（旧->新顺序）重挂到目标兄弟。
// 调用前提：源节点链头已被 consolidate 的 CAS 锁定，这些增量不会再被并发追加，转发无竞态。
func (t *Tree) forwardDeltasToSibling(wid uint32, deltas []*Record, siblingPID PID) {
	t.forwardDeltasToSiblingReliable(wid, deltas, siblingPID)
}

// forwardDeltasToSiblingReliable 逐条把增量 CAS 头插到兄弟，返回是否全部成功。
// 持有兄弟的写门闩，与兄弟上的并发写互斥，保证转发的增量落在确定的链头上。
func (t *Tree) forwardDeltasToSiblingReliable(wid uint32, deltas []*Record, siblingPID PID) bool {
	if len(deltas) == 0 {
		return true
	}
	latch := t.latchFor(siblingPID)
	latch.Lock()
	defer latch.Unlock()
	for _, src := range deltas {
		delta := t.allocRecord(wid, src.kind)
		delta.key, delta.val = src.key, src.val
		sibHead := t.mapping.load(siblingPID)
		if sibHead == nil {
			t.releaseRecord(wid, delta)
			return false
		}
		delta.next = sibHead
		if !t.mapping.cas(siblingPID, sibHead, delta) {
			t.releaseRecord(wid, delta)
			t.stats[wid].casRetry++
			return false
		}
	}
	return true
}

// ---- 内层节点分裂（两阶段，结构同叶子）----

func (t *Tree) splitInternal(wid uint32, leftPID PID, leftBase *Record) {
	for attempt := 0; attempt < 16; attempt++ {
		head := t.mapping.load(leftPID)
		view := walkChain(head)
		if view.merge != nil {
			t.stats[wid].mergeAbort++
			return
		}
		if view.split != nil {
			t.helpSplit(wid, leftPID, view.split)
			return
		}
		if view.base == nil || view.base.kind != recInternalBase {
			return
		}
		base := view.base
		n := len(base.ip.entries)
		if n < leafSplitAt {
			return
		}
		// 同叶子分裂：仅在纯基础记录上分裂，避免并发路由增量在裁剪时丢失。
		if view.chainLen > 1 {
			if nb, _, ok := t.consolidate(wid, leftPID, head); ok {
				t.postConsolidate(wid, leftPID, nb)
			}
			return
		}
		mid := n / 2
		splitKey := base.ip.entries[mid].lo
		rightEntries := append([]routeEntry(nil), base.ip.entries[mid:]...)

		rightBase := t.allocRecord(wid, recInternalBase)
		rightBase.owner = wid
		rightBase.lo, rightBase.hi = splitKey, base.hi
		rightBase.ip = &internalPayload{entries: rightEntries}
		rightPID := t.mapping.allocPID(rightBase)
		t.markLive(rightPID)

		split := t.allocRecord(wid, recSplit)
		split.sib = rightPID
		split.hi = splitKey
		split.lo = base.lo
		split.next = head
		parentPID := t.findParent(wid, leftPID, splitKey)
		split.parent = parentPID
		if !t.mapping.cas(leftPID, head, split) {
			t.releaseRecord(wid, split)
			t.mapping.freePID(rightPID)
			t.unmarkLive(rightPID)
			t.releaseRecord(wid, rightBase)
			t.stats[wid].casRetry++
			continue
		}
		t.logEvent(TimelineEvent{Kind: "split", WID: wid, Node: leftPID, Sibling: rightPID,
			Parent: parentPID, SplitKey: splitKey, Detail: "内层节点分裂第一步：挂分裂增量"})
		leftHead := t.mapping.load(leftPID)
		if _, _, ok := t.consolidate(wid, leftPID, leftHead); !ok {
			return
		}
		if parentPID == leftPID {
			t.finishRootSplit(wid, leftPID, split, rightPID, splitKey)
		} else {
			t.helpSplit(wid, leftPID, split)
		}
		return
	}
}

// finishRootSplit 处理根分裂：造一个新的内部根，把左右两个孩子挂进去，
// 再用 CAS 替换全局根指针。
func (t *Tree) finishRootSplit(wid uint32, oldRootPID PID, split *Record, rightPID PID, splitKey Key) {
	leftLo := split.lo
	oldHi := split.nextRangeHi()

	newRoot := t.allocRecord(wid, recInternalBase)
	newRoot.owner = wid
	newRoot.lo, newRoot.hi = leftLo, oldHi
	newRoot.ip = &internalPayload{entries: []routeEntry{
		{child: oldRootPID, lo: leftLo, hi: splitKey},
		{child: rightPID, lo: splitKey, hi: oldHi},
	}}
	newRootPID := t.mapping.allocPID(newRoot)
	t.markLive(newRootPID)
	if !t.rootPID.CompareAndSwap(uint64(oldRootPID), uint64(newRootPID)) {
		// 别的线程已完成根分裂：新根中应已包含右兄弟，释放本次产物。
		t.mapping.freePID(newRootPID)
		t.unmarkLive(newRootPID)
		t.releaseRecord(wid, newRoot)
		return
	}
	// 旧根 PID 仍是活节点（左孩子），不回收；分裂增量的父指针语义上指向新根。
	split.parent = newRootPID
	t.logEvent(TimelineEvent{Kind: "split", WID: wid, Node: oldRootPID, Sibling: rightPID,
		Parent: newRootPID, SplitKey: splitKey, Detail: "根分裂第二步：安装新内部根"})
}

// findParent 从根路由到 key 所在叶子的父内层节点。
// 若 pid 当前就是根，返回 pid 自身（调用方据此走根分裂流程）。
func (t *Tree) findParent(wid uint32, pid PID, key Key) PID {
	if PID(t.rootPID.Load()) == pid {
		return pid
	}
	rootPID := PID(t.rootPID.Load())
	cur := rootPID
	for loop := 0; loop < 64; loop++ {
		head := t.mapping.load(cur)
		v := walkChain(head)
		if v.base == nil || v.base.kind == recLeafBase {
			return rootPID // 防御：找不到就回根
		}
		if v.split != nil {
			t.helpSplit(wid, cur, v.split)
			head = t.mapping.load(cur)
		}
		next := routeOnce(head, key)
		if next == pid || next == invalidPID {
			return cur
		}
		cur = next
	}
	return rootPID
}

// ---- 叶子合并（三阶段，遇并发分裂按先后放弃一方）----

func (t *Tree) tryMergeLeaf(wid uint32, pid PID, base *Record) {
	if base.lo == 0 && base.hi == MaxKey {
		return // 唯一根叶子，无可合并对象
	}
	for attempt := 0; attempt < 8; attempt++ {
		head := t.mapping.load(pid)
		view := walkChain(head)
		if view.split != nil {
			// 本节点已有分裂在先：合并放弃。
			t.stats[wid].mergeAbort++
			t.logEvent(TimelineEvent{Kind: "merge-abort", WID: wid, Node: pid,
				Detail: "合并遇已挂分裂增量，按先后规则放弃合并"})
			return
		}
		if view.merge != nil {
			return // 已有合并流程在身
		}
		if view.base == nil || view.base.kind != recLeafBase {
			return
		}
		base = view.base

		// 找左兄弟与父节点。
		leftPID, parentPID := t.findLeftSibling(wid, pid, base.lo)
		if leftPID == invalidPID || parentPID == invalidPID {
			return
		}
		leftHead := t.mapping.load(leftPID)
		lv := walkChain(leftHead)
		if lv.split != nil || lv.merge != nil || lv.base == nil {
			return // 左兄弟正在结构变更，本轮让开
		}
		// 合并后仍可能超阈值则不做（说明左兄弟已经很大）。
		if len(lv.base.lp.keys)+len(base.lp.keys) >= leafSplitAt {
			return
		}

		// 第一步：在右节点（本节点）挂合并增量，读者据此直接跳到左兄弟。
		merge := t.allocRecord(wid, recMerge)
		merge.sib = leftPID
		merge.parent = parentPID
		merge.hi = base.lo // 分隔键
		merge.lo = base.lo
		merge.next = head
		if !t.mapping.cas(pid, head, merge) {
			t.releaseRecord(wid, merge)
			t.stats[wid].casRetry++
			continue
		}
		t.logEvent(TimelineEvent{Kind: "merge", WID: wid, Node: pid, Sibling: leftPID,
			Parent: parentPID, SplitKey: base.lo, Detail: "合并第一步：右节点挂合并增量"})

		// 第二步：把右节点内容并进左兄弟（物化左+右，一条新基础记录 CAS 上去）。
		if !t.mergeIntoLeft(wid, leftPID, pid) {
			t.abortMerge(wid, pid, merge)
			return
		}
		// 第三步：在父节点挂删除路由增量，使后续选路不再进入右节点。
		// 右节点 PID 此刻不回收，保留为带 merge 增量的空壳：持有旧指针的线程经 merge.sib
		// 跳到左兄弟，不会有指向 nil 的悬空路由，也不产生左右范围重叠。
		// 物理回收统一推迟，由父节点后续物化/段整理时在无读者时进行。
		if !t.removeParentRoute(wid, parentPID, pid, base.lo) {
			// 父路由清理失败（父节点已不存在等）：放弃合并，恢复右节点独立。
			t.abortMerge(wid, pid, merge)
			return
		}
		t.logEvent(TimelineEvent{Kind: "merge", WID: wid, Node: pid, Sibling: leftPID,
			Parent: parentPID, SplitKey: base.lo,
			Detail: "合并完成：数据并入左兄弟，右节点保留为空壳（merge 增量跳转）"})
		return
	}
}

// mergeIntoLeft 物化“左兄弟 + 右节点”的全部存活键值，替换左兄弟基础记录。
// 若期间左兄弟出现分裂增量，返回 false（调用方放弃本次合并）。
func (t *Tree) mergeIntoLeft(wid uint32, leftPID, rightPID PID) bool {
	for attempt := 0; attempt < 16; attempt++ {
		leftHead := t.mapping.load(leftPID)
		lv := walkChain(leftHead)
		if lv.split != nil {
			t.stats[wid].mergeAbort++
			t.logEvent(TimelineEvent{Kind: "merge-abort", WID: wid, Node: leftPID,
				Detail: "并入途中左兄弟发生分裂，放弃合并"})
			return false
		}
		if lv.merge != nil || lv.base == nil {
			return false
		}
		rightHead := t.mapping.load(rightPID)
		rv := walkChain(rightHead)
		if rv.base == nil {
			return false
		}
		// 收集两边存活键值：分别用各自的视图范围物化，再归并。
		type kv struct {
			k Key
			v Value
		}
		all := make([]kv, 0, len(lv.base.lp.keys)+len(rv.base.lp.keys)+8)
		collect := func(head *Record, view chainView) {
			b, _ := materializeLeaf(head, view.lo, view.hi)
			for i, k := range b.lp.keys {
				all = append(all, kv{k, b.lp.vals[i]})
			}
		}
		collect(leftHead, lv)
		collect(rightHead, rv)
		sort.Slice(all, func(i, j int) bool { return all[i].k < all[j].k })
		// 去重（理论上左右范围互斥，防御性保留后者）。
		keys := make([]Key, 0, len(all))
		vals := make([]Value, 0, len(all))
		for i, e := range all {
			if i > 0 && all[i-1].k == e.k {
				vals[len(vals)-1] = e.v
				continue
			}
			keys = append(keys, e.k)
			vals = append(vals, e.v)
		}
		newBase := t.allocRecord(wid, recLeafBase)
		newBase.owner = wid
		newBase.lo, newBase.hi = lv.lo, rv.hi
		newBase.lp = &leafPayload{keys: keys, vals: vals}
		if !t.mapping.cas(leftPID, leftHead, newBase) {
			t.releaseRecord(wid, newBase)
			t.stats[wid].casRetry++
			continue
		}
		for r := leftHead; r != nil; r = r.next {
			t.decCount(r.kind)
		}
		t.epoch.retire(leftHead, invalidPID)
		return true
	}
	return false
}

// abortMerge 在右节点链上挂“放弃合并”增量，使旧合并增量失效。
func (t *Tree) abortMerge(wid uint32, pid PID, merge *Record) {
	head := t.mapping.load(pid)
	ab := t.allocRecord(wid, recAbortMerge)
	ab.next = head
	if t.mapping.cas(pid, head, ab) {
		t.logEvent(TimelineEvent{Kind: "merge-abort", WID: wid, Node: pid,
			Detail: "挂放弃合并增量，节点恢复独立"})
	} else {
		t.releaseRecord(wid, ab)
	}
}

// removeParentRoute 在父节点上挂分隔键删除增量，幂等。返回是否确认路由已摘除。
func (t *Tree) removeParentRoute(wid uint32, parentPID, childPID PID, sep Key) bool {
	for attempt := 0; attempt < 16; attempt++ {
		head := t.mapping.load(parentPID)
		if head == nil {
			return false
		}
		v := walkChain(head)
		if v.base == nil || v.base.kind != recInternalBase {
			return false
		}
		// 链上已存在等价删除增量，或基础/增量里已无此孩子，都视为已摘除。
		if !internalHasChild(head, childPID) {
			return true
		}
		rm := t.allocRecord(wid, recRemoveKey)
		rm.child = childPID
		rm.lo = sep
		rm.hi = v.hi
		rm.next = head
		if t.mapping.cas(parentPID, head, rm) {
			t.logEvent(TimelineEvent{Kind: "merge", WID: wid, Node: childPID,
				Parent: parentPID, SplitKey: sep, Detail: "合并第三步：父节点删除分隔键"})
			return true
		}
		t.releaseRecord(wid, rm)
		t.stats[wid].casRetry++
	}
	return false
}

// findLeftSibling 从根路由，返回 pid 在父节点上紧邻的左兄弟 PID 与父 PID。
func (t *Tree) findLeftSibling(wid uint32, pid PID, key Key) (PID, PID) {
	rootPID := PID(t.rootPID.Load())
	cur := rootPID
	for loop := 0; loop < 64; loop++ {
		head := t.mapping.load(cur)
		v := walkChain(head)
		if v.base == nil {
			return invalidPID, invalidPID
		}
		if v.base.kind == recLeafBase {
			return invalidPID, invalidPID
		}
		if v.split != nil {
			t.helpSplit(wid, cur, v.split)
			head = t.mapping.load(cur)
		}
		// 汇总当前内层节点的全部路由项（增量优先），按 lo 排序后找 pid 的前一个。
		seen := make(map[PID]routeEntry)
		for x := head; x != nil; x = x.next {
			if x.kind == recIndexEntry {
				seen[x.child] = routeEntry{child: x.child, lo: x.lo, hi: x.hi}
			}
			if x.kind == recInternalBase {
				for _, e := range x.ip.entries {
					if _, ok := seen[e.child]; !ok {
						seen[e.child] = e
					}
				}
			}
		}
		entries := make([]routeEntry, 0, len(seen))
		for _, e := range seen {
			entries = append(entries, e)
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].lo < entries[j].lo })
		idx := -1
		for i, e := range entries {
			if e.child == pid {
				idx = i
				break
			}
		}
		child := routeOnce(head, key)
		if child == invalidPID {
			return invalidPID, invalidPID
		}
		if idx >= 0 {
			if idx == 0 {
				return invalidPID, cur // 已是最左孩子
			}
			return entries[idx-1].child, cur
		}
		cur = child
	}
	return invalidPID, invalidPID
}
