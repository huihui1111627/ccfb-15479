package bwtree

import "sort"

// ---- 叶子基础负载（数组有序，物化时重建）----

func (p *leafPayload) get(k Key) (Value, bool) {
	i := sort.Search(len(p.keys), func(i int) bool { return p.keys[i] >= k })
	if i < len(p.keys) && p.keys[i] == k {
		return p.vals[i], true
	}
	return Value{}, false
}

// ---- 内部节点基础负载 ----

// route 在基础路由表上为 key 选路。
func (p *internalPayload) route(k Key) PID {
	i := sort.Search(len(p.entries), func(i int) bool { return p.entries[i].lo > k })
	if i == 0 {
		if len(p.entries) > 0 {
			return p.entries[0].child
		}
		return invalidPID
	}
	return p.entries[i-1].child
}

// routeExcept 同 route，但跳过 removed 集合中的孩子（对应删除增量或本次判失效）。
// 基础表按 lo 有序，取满足 lo<=k 的最右一个未屏蔽孩子；都不在范围则取最左未屏蔽者。
func (p *internalPayload) routeExcept(k Key, removed map[PID]bool) PID {
	var best PID
	found := false
	for _, e := range p.entries {
		if removed[e.child] {
			continue
		}
		if e.lo <= k {
			best = e.child
			found = true
		} else if !found {
			// k 落在所有未屏蔽孩子之前：返回最左的未屏蔽孩子。
			return e.child
		}
	}
	if found {
		return best
	}
	return invalidPID
}

// materializeLeaf 把一条叶子增量链物化成新的基础记录。
// 从基础负载出发，按增量从旧到新回放（反向遍历链），产出有序键值数组。
// 第二个返回值是被 [lo,hi) 范围裁剪排除、按旧->新排列的数据增量（分裂窗口的错位写），
// 调用方负责把它们转发到对应兄弟节点，不能丢弃。
func materializeLeaf(head *Record, lo, hi Key) (*Record, []*Record) {
	base := (*Record)(nil)
	// 收集增量（头最新、尾最旧），反向即旧->新。
	deltas := make([]*Record, 0, 16)
	for r := head; r != nil && r.kind != recLeafBase; r = r.next {
		deltas = append(deltas, r)
	}
	for r := head; r != nil; r = r.next {
		if r.kind == recLeafBase {
			base = r
			break
		}
	}
	keys := make([]Key, 0)
	vals := make([]Value, 0)
	if base != nil {
		keys = append(keys, base.lp.keys...)
		vals = append(vals, base.lp.vals...)
	}
	upsert := func(k Key, v Value, blind bool) {
		i := sort.Search(len(keys), func(i int) bool { return keys[i] >= k })
		if i < len(keys) && keys[i] == k {
			vals[i] = v
			return
		}
		if blind && i < len(keys) && keys[i] == k {
			vals[i] = v
			return
		}
		keys = append(keys, 0)
		copy(keys[i+1:], keys[i:])
		vals = append(vals, Value{})
		copy(vals[i+1:], vals[i:])
		keys[i], vals[i] = k, v
	}
	remove := func(k Key) {
		i := sort.Search(len(keys), func(i int) bool { return keys[i] >= k })
		if i < len(keys) && keys[i] == k {
			keys = append(keys[:i], keys[i+1:]...)
			vals = append(vals[:i], vals[i+1:]...)
		}
	}
	// 数据增量一律在本节点回放，绝不按范围转走——增量是已确认的写入，转走会在嵌套分裂/
	// 转发失败时丢写。范围收窄只裁剪基础负载里的静态副本；分裂切分由 splitLeaf 用基础快照
	// 在创建右兄弟时一次性完成，新写入始终落在当前权威叶子。
	excluded := make([]*Record, 0)
	for i := len(deltas) - 1; i >= 0; i-- {
		d := deltas[i]
		switch d.kind {
		case recInsert, recUpdate, recBlindWrite:
			upsert(d.key, d.val, d.kind == recBlindWrite)
		case recDelete:
			remove(d.key)
		case recAbortMerge:
			// 纯标记，物化时丢弃。
		default:
			// 结构增量（split/merge/index...）不进叶子负载。
		}
	}
	// 仅裁剪基础负载的静态副本到责任范围；这些键在分裂创建兄弟时已复制过去，裁剪不丢数据。
	if len(keys) > 0 {
		first := sort.Search(len(keys), func(i int) bool { return keys[i] >= lo })
		last := sort.Search(len(keys), func(i int) bool { return keys[i] >= hi })
		keys = keys[first:last]
		vals = vals[first:last]
	}
	return &Record{
		kind: recLeafBase,
		lo:   lo,
		hi:   hi,
		lp:   &leafPayload{keys: keys, vals: vals},
	}, excluded
}

// materializeInternal 把内层节点增量链物化成新的基础记录。
func materializeInternal(head *Record, lo, hi Key) *Record {
	base := (*Record)(nil)
	deltas := make([]*Record, 0, 8)
	for r := head; r != nil && r.kind != recInternalBase; r = r.next {
		deltas = append(deltas, r)
	}
	for r := head; r != nil; r = r.next {
		if r.kind == recInternalBase {
			base = r
			break
		}
	}
	entries := make([]routeEntry, 0)
	if base != nil {
		entries = append(entries, base.ip.entries...)
	}
	// 按 child 维护最新范围，删除键增量标记移除。
	type state struct {
		entry   routeEntry
		removed bool
	}
	byChild := make(map[PID]int)
	for i := range entries {
		byChild[entries[i].child] = i
	}
	for i := len(deltas) - 1; i >= 0; i-- {
		d := deltas[i]
		switch d.kind {
		case recIndexEntry:
			if idx, ok := byChild[d.child]; ok {
				entries[idx] = routeEntry{child: d.child, lo: d.lo, hi: d.hi}
			} else {
				byChild[d.child] = len(entries)
				entries = append(entries, routeEntry{child: d.child, lo: d.lo, hi: d.hi})
			}
		case recRemoveKey:
			if idx, ok := byChild[d.child]; ok {
				entries = append(entries[:idx], entries[idx+1:]...)
				delete(byChild, d.child)
				// 重建索引。
				for j := range entries {
					byChild[entries[j].child] = j
				}
			}
		case recAbortMerge:
			// 纯标记，物化时丢弃。
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].lo < entries[j].lo })
	return &Record{
		kind: recInternalBase,
		lo:   lo,
		hi:   hi,
		ip:   &internalPayload{entries: entries},
	}
}
