// Package refmap 是一个用全局读写锁保护的有序映射，作为无锁索引的正确性对照。
package refmap

import (
	"sort"
	"sync"

	"bwtea/pkg/bwtree"
)

type node struct {
	key      bwtree.Key
	val      bwtree.Value
	priority uint64
	left     *node
	right    *node
}

// Map 是全局锁有序映射（treap）。
type Map struct {
	mu sync.RWMutex
	rt *node
}

func New() *Map { return &Map{} }

func rotateRight(x *node) *node {
	y := x.left
	x.left = y.right
	y.right = x
	return y
}

func rotateLeft(x *node) *node {
	y := x.right
	x.right = y.left
	y.left = x
	return y
}

func insert(n *node, k bwtree.Key, v bwtree.Value, prio uint64) *node {
	if n == nil {
		return &node{key: k, val: v, priority: prio}
	}
	if k == n.key {
		n.val = v
		return n
	}
	if k < n.key {
		n.left = insert(n.left, k, v, prio)
		if n.left.priority > n.priority {
			n = rotateRight(n)
		}
	} else {
		n.right = insert(n.right, k, v, prio)
		if n.right.priority > n.priority {
			n = rotateLeft(n)
		}
	}
	return n
}

func erase(n *node, k bwtree.Key) *node {
	if n == nil {
		return nil
	}
	if k < n.key {
		n.left = erase(n.left, k)
	} else if k > n.key {
		n.right = erase(n.right, k)
	} else {
		if n.left == nil {
			return n.right
		}
		if n.right == nil {
			return n.left
		}
		if n.left.priority < n.right.priority {
			n = rotateLeft(n)
			n.left = erase(n.left, k)
		} else {
			n = rotateRight(n)
			n.right = erase(n.right, k)
		}
	}
	return n
}

func search(n *node, k bwtree.Key) (bwtree.Value, bool) {
	for n != nil {
		if k == n.key {
			return n.val, true
		}
		if k < n.key {
			n = n.left
		} else {
			n = n.right
		}
	}
	return bwtree.Value{}, false
}

func appendInOrder(n *node, lo, hi bwtree.Key, out *[]bwtree.KV) {
	if n == nil {
		return
	}
	if n.key >= lo {
		appendInOrder(n.left, lo, hi, out)
	}
	if n.key >= lo && n.key < hi {
		*out = append(*out, bwtree.KV{Key: n.key, Val: n.val})
	}
	if n.key < hi {
		appendInOrder(n.right, lo, hi, out)
	}
}

// Upsert 加锁插入或更新。
func (m *Map) Upsert(k bwtree.Key, v bwtree.Value) {
	m.mu.Lock()
	m.rt = insert(m.rt, k, v, nextPrio())
	m.mu.Unlock()
}

// Delete 加锁删除。
func (m *Map) Delete(k bwtree.Key) {
	m.mu.Lock()
	m.rt = erase(m.rt, k)
	m.mu.Unlock()
}

// Get 加读锁点查。
func (m *Map) Get(k bwtree.Key) (bwtree.Value, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return search(m.rt, k)
}

// Scan 加读锁范围扫描。
func (m *Map) Scan(lo, hi bwtree.Key) []bwtree.KV {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]bwtree.KV, 0)
	appendInOrder(m.rt, lo, hi, &out)
	return out
}

// Snapshot 返回全量有序键值切片（最终内容对比用）。
func (m *Map) Snapshot() []bwtree.KV {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]bwtree.KV, 0)
	appendInOrder(m.rt, 0, bwtree.MaxKey, &out)
	return out
}

// Size 返回键数量。
func (m *Map) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var count int
	var walk func(*node)
	walk = func(n *node) {
		if n == nil {
			return
		}
		count++
		walk(n.left)
		walk(n.right)
	}
	walk(m.rt)
	return count
}

// SortedKeys 仅供检查器复用：返回排序后的键集合。
func SortedKeys(kvs []bwtree.KV) []bwtree.Key {
	keys := make([]bwtree.Key, len(kvs))
	for i, kv := range kvs {
		keys[i] = kv.Key
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}
