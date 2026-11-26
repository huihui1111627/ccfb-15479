package bwtree

import "sync/atomic"

// deltaCounters 按类型统计全树增量记录数量（原子计数）。
type deltaCounters struct {
	c [kindCount]atomic.Uint64
}

// allocRecord 从本线程池取一条定长记录并登记类型计数。
func (t *Tree) allocRecord(wid uint32, kind uint8) *Record {
	r := t.pools[wid].get(wid)
	r.kind = kind
	t.deltaCounters.c[kind].Add(1)
	return r
}

// countRecord 对未经 allocRecord 路径创建的记录（如 New 的根基础记录）补登记。
func (t *Tree) countRecord(r *Record) {
	if r != nil {
		t.deltaCounters.c[r.kind].Add(1)
	}
}

// decCount 安全递减某类型计数，带下限保护避免下溢成巨大无符号数。
func (t *Tree) decCount(kind uint8) {
	c := &t.deltaCounters.c[kind]
	for {
		v := c.Load()
		if v == 0 {
			return // 计数与回收在结构竞争下可能短暂失配，下限保护即可
		}
		if c.CompareAndSwap(v, v-1) {
			return
		}
	}
}

// releaseRecord 把一条不再需要的记录（CAS 失败或帮助路径放弃）直接退回本线程池，
// 不走任何全局分配器。
func (t *Tree) releaseRecord(wid uint32, r *Record) {
	if r == nil {
		return
	}
	t.decCount(r.kind)
	t.pools[wid].put(r)
}

// Get 点查。
func (t *Tree) Get(wid uint32, k Key) (Value, bool) {
	t.enter(wid)
	defer t.leave(wid)
	t.stats[wid].opsGet++
	for attempt := 0; attempt < 16; attempt++ {
		pid, head := t.locateLeaf(wid, k)
		view := walkChain(head)
		// 分裂挂起：协助收敛后重读，不在变迁窗口读旧副本。
		if view.split != nil {
			t.helpSplit(wid, pid, view.split)
			t.stats[wid].locRestart++
			continue
		}
		// 责任范围不含 k（陈旧路由/收窄边界）：回根重定位。
		if k < view.lo || k >= view.hi {
			t.stats[wid].locRestart++
			continue
		}
		// 在叶子写门闩保护下读稳定快照；拿不到锁说明正有写/结构变迁，重读而非读潜在旧值。
		latch := t.latchFor(pid)
		if !latch.TryLock() {
			t.stats[wid].locRestart++
			continue
		}
		cur := t.mapping.load(pid)
		cv := walkChain(cur)
		latch.Unlock()
		if cv.split == nil && k >= cv.lo && k < cv.hi {
			return leafLookup(cur, k)
		}
		t.stats[wid].locRestart++
		continue
	}
	// 兜底：强制从根定位到一个活叶子再读。
	_, head := t.forceLocateLeaf(k)
	return leafLookup(head, k)
}

// Put 插入或更新（先查后写：键已存在则更新，否则插入）。
func (t *Tree) Put(wid uint32, k Key, v Value) {
	t.enter(wid)
	defer t.leave(wid)
	t.stats[wid].opsInsert++
	_, existed := t.writeLeaf(wid, k, v, false)
	if existed {
		t.stats[wid].opsInsert--
		t.stats[wid].opsUpdate++
	}
}

// BlindPut 只写不读：不先查键是否存在，直接挂盲写增量。
// 查找时碰到盲写记录直接取它的值。
func (t *Tree) BlindPut(wid uint32, k Key, v Value) {
	t.enter(wid)
	defer t.leave(wid)
	t.stats[wid].opsBlind++
	t.writeLeaf(wid, k, v, true)
}

// Delete 删除键；删除增量挂在链上，查找时命中即视为不存在。
func (t *Tree) Delete(wid uint32, k Key) {
	t.enter(wid)
	defer t.leave(wid)
	t.stats[wid].opsDelete++
	t.deleteLeaf(wid, k)
}

// writeLeaf 定位叶子并挂一条数据增量，CAS 失败则重读重来。
// 返回 (是否重试了结构维护, 键此前是否存在)。
func (t *Tree) writeLeaf(wid uint32, k Key, v Value, blind bool) (bool, bool) {
	for loop := 0; loop < maxLoops; loop++ {
		pid, head := t.locateLeaf(wid, k)
		view := walkChain(head)
		if view.merge != nil {
			t.stats[wid].locRestart++
			continue // 合并中，locate 下一轮会跳到左兄弟
		}
		if view.split != nil {
			// 分裂挂起：协助完成后整体重定位，不在变迁中的节点写。
			t.stats[wid].locRestart++
			t.helpSplit(wid, pid, view.split)
			continue
		}
		// 叶子写门闩：把同一叶子的数据写与它的物化/分裂互斥，关闭分裂窗口的写错节点竞态。
		latch := t.latchFor(pid)
		latch.Lock()
		freshHead := t.mapping.load(pid)
		freshView := walkChain(freshHead)
		if freshHead != head || freshView.split != nil || freshView.merge != nil ||
			k < freshView.lo || k >= freshView.hi {
			latch.Unlock()
			t.stats[wid].casRetry++
			continue // 重读期间节点已变更，或该叶子并不负责 k：重新定位
		}
		kind := recInsert
		existed := false
		if !blind {
			if _, ok := leafLookup(freshHead, k); ok {
				kind = recUpdate
				existed = true
			}
		} else {
			kind = recBlindWrite
		}
		delta := t.allocRecord(wid, kind)
		delta.key, delta.val = k, v
		delta.next = freshHead
		if !t.mapping.cas(pid, freshHead, delta) {
			latch.Unlock()
			t.releaseRecord(wid, delta)
			t.stats[wid].casRetry++
			t.maybeAdvanceEpoch(wid)
			continue
		}
		latch.Unlock()
		// 结构维护（物化/分裂）在锁外自行通过门闩与后续写互斥；失败不影响本次已完成的写入。
		t.maintainAfterWrite(wid, pid, delta)
		return false, existed
	}
	panic("bwtree: writeLeaf exceeded max loops")
}

// deleteLeaf 是 Delete 的实现，单独命名以便语义清晰。
func (t *Tree) deleteLeaf(wid uint32, k Key) {
	for loop := 0; loop < maxLoops; loop++ {
		pid, head := t.locateLeaf(wid, k)
		view := walkChain(head)
		if view.merge != nil {
			t.stats[wid].locRestart++
			continue
		}
		if view.split != nil {
			t.stats[wid].locRestart++
			t.helpSplit(wid, pid, view.split)
			continue
		}
		latch := t.latchFor(pid)
		latch.Lock()
		freshHead := t.mapping.load(pid)
		freshView := walkChain(freshHead)
		if freshHead != head || freshView.split != nil || freshView.merge != nil ||
			k < freshView.lo || k >= freshView.hi {
			latch.Unlock()
			t.stats[wid].casRetry++
			continue
		}
		delta := t.allocRecord(wid, recDelete)
		delta.key = k
		delta.next = freshHead
		if !t.mapping.cas(pid, freshHead, delta) {
			latch.Unlock()
			t.releaseRecord(wid, delta)
			t.stats[wid].casRetry++
			t.maybeAdvanceEpoch(wid)
			continue
		}
		latch.Unlock()
		t.maintainAfterWrite(wid, pid, delta)
		return
	}
	panic("bwtree: deleteLeaf exceeded max loops")
}

// maybeAdvanceEpoch 在线程遇到竞争时顺手尝试推进 epoch，驱动垃圾回收。
func (t *Tree) maybeAdvanceEpoch(wid uint32) {
	if t.epoch.tryAdvance() {
		t.stats[wid].epochAdvance++
	}
}

// maintainAfterWrite 在成功写入后检查链长/节点大小并做结构维护。
func (t *Tree) maintainAfterWrite(wid uint32, pid PID, head *Record) {
	view := walkChain(head)
	if view.merge != nil || view.split != nil {
		return // 结构变更进行中，交由对应流程收尾
	}
	if view.chainLen < chainLimit {
		return
	}
	// 锁外维护：通过带门闩的物化与其他写者互斥（TryLock 拿不到就跳过，下个写者会再触发）。
	newBase, _, changed := t.consolidateWithLatch(wid, pid, head)
	if !changed {
		return
	}
	t.postConsolidate(wid, pid, newBase)
}
