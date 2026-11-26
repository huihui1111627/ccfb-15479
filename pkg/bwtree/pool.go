package bwtree

import "sync"

// recPool 是单个线程自己的固定大小记录池。
//
// 增量记录统一为定长 Record，优先从本线程空闲链表重用；
// CAS 失败的增量不经过任何全局分配器，直接退回本池。
// epoch 回收的旧记录按 owner 字段退回原属池，因此 put 需带锁以支持跨线程归还。
type recPool struct {
	mu        sync.Mutex
	free      []*Record
	alloc     uint64 // 累计新分配次数
	reuse     uint64 // 累计重用次数（仅未发布记录）
	reclaimed uint64 // 已发布记录被 epoch 逻辑回收的次数（不物理重用）
}

// MaxWorkers 是索引支持的最大工作线程数（统计/epoch 通告数组按此定长）。
const MaxWorkers = 64

func newRecPool() *recPool {
	return &recPool{}
}

// get 取一条清零后的记录，并登记属主线程。
func (p *recPool) get(wid uint32) *Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.free); n > 0 {
		r := p.free[n-1]
		p.free = p.free[:n-1]
		*r = Record{owner: wid}
		p.reuse++
		return r
	}
	p.alloc++
	return &Record{owner: wid}
}

// put 把一条“从未发布”的记录退回池中重用（仅 CAS 失败的增量可走这里）。
// 已发布记录由 epoch 回收调 noteReclaimed，不物理重用，避免跨链环。
func (p *recPool) put(r *Record) {
	if r == nil {
		return
	}
	p.mu.Lock()
	p.free = append(p.free, r)
	p.mu.Unlock()
}

// noteReclaimed 仅记账：一条已发布记录在 epoch 隔离后被逻辑回收，内存交给 GC。
func (p *recPool) noteReclaimed() {
	p.mu.Lock()
	p.reclaimed++
	p.mu.Unlock()
}

func (p *recPool) stats() (freeLen, alloc, reuse uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return uint64(len(p.free)), p.alloc, p.reuse
}
