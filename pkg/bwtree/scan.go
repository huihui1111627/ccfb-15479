package bwtree

// Scan 顺序扫描键范围 [lo, hi)。
//
// 正确性策略：维护单调不减的 cursor，每轮用 locateLeaf 找到负责 cursor 的叶子，
// 交付其中 >= cursor 的键并把 cursor 推进到“最大已交付键 +1”。若该叶子没有可交付的键，
// cursor 严格 +1。这样每个键值都不会被跳过；emitted 集合负责在叶子范围短暂重叠时去重。
// 终止性：cursor 每轮严格增大，最多推进 hi-lo 次即结束，不依赖可能滞后的节点 hi。
func (t *Tree) Scan(wid uint32, lo, hi Key) []KV {
	t.enter(wid)
	defer t.leave(wid)
	t.stats[wid].opsScan++
	if lo >= hi {
		return nil
	}
	out := make([]KV, 0, 32)
	emitted := make(map[Key]bool, 32)
	cursor := lo
	for iter := 0; iter < maxLoops; iter++ {
		if cursor >= hi {
			return out
		}
		_, head := t.locateLeaf(wid, cursor)
		view := walkChain(head)
		if view.merge != nil {
			// 合并空壳：locateLeaf 正常应已跳左兄弟；若仍拿到空壳，+1 保证推进。
			t.stats[wid].scanRestart++
			cursor++
			continue
		}
		base, _ := materializeLeaf(head, view.lo, view.hi)
		advanced := false
		var maxKey Key
		for i, k := range base.lp.keys {
			if k >= hi {
				return out
			}
			if k >= cursor {
				if !emitted[k] {
					out = append(out, KV{Key: k, Val: base.lp.vals[i]})
					emitted[k] = true
				}
				if !advanced || k > maxKey {
					maxKey = k
				}
				advanced = true
			}
		}
		if advanced {
			cursor = maxKey + 1
		} else {
			// 叶子不含 >= cursor 的键。若该叶子的责任上界有效且在 cursor 之后，直接跳到上界，
			// 跳过整段空区间；否则严格 +1。两种情况都保证 cursor 单调增大、不丢键。
			if view.hi > cursor {
				cursor = view.hi
			} else {
				cursor++
			}
			t.stats[wid].scanRestart++
		}
	}
	panic("bwtree: scan exceeded max loops")
}
