package bwtree

import "sync/atomic"

// epochMgr 是一个简化的 epoch 回收器（QSBR）。
//
// 线程在访问任何映射表项前后 enter/leave 通告当前 epoch；
// 被替换下来的旧记录链、被合并掉的节点进入 limbo，待两个 epoch 之后
// 确认没有线程还持有旧指针，才把记录退回原属池、逻辑号空闲复用。
type epochMgr struct {
	current atomic.Uint64
	ann     [MaxWorkers]atomic.Uint64 // 每个线程的通告 epoch，0 表示线程不在临界区
	limbo   chan limboItem
	// 回收动作交回 Tree 执行（退池、释放 PID），用回调避免循环依赖。
	reclaim func(item limboItem)
}

// limboItem 是等待回收的一批垃圾。
type limboItem struct {
	retireEpoch uint64
	records     []*Record // 整链记录，按 owner 退回各自线程池
	freePID     PID      // 若整节点废弃，其逻辑号；否则为 0
}

func newEpochMgr(reclaim func(limboItem)) *epochMgr {
	e := &epochMgr{reclaim: reclaim}
	e.current.Store(1)
	e.limbo = make(chan limboItem, 4096)
	return e
}

// enter 通告线程进入临界区，返回当前全局 epoch。
func (e *epochMgr) enter(wid uint32) uint64 {
	ep := e.current.Load()
	e.ann[wid].Store(ep)
	// 再读一次，防止与 advance 交错。
	if cur := e.current.Load(); cur != ep {
		e.ann[wid].Store(cur)
		return cur
	}
	return ep
}

// leave 通告线程离开临界区。
func (e *epochMgr) leave(wid uint32) {
	e.ann[wid].Store(0)
}

// retire 把一条旧记录链（可选附带废弃 PID）放入 limbo。
func (e *epochMgr) retire(chainHead *Record, freePID PID) {
	if chainHead == nil && freePID == invalidPID {
		return
	}
	item := limboItem{retireEpoch: e.current.Load(), freePID: freePID}
	for r := chainHead; r != nil; r = r.next {
		item.records = append(item.records, r)
	}
	e.limbo <- item
}

// tryAdvance 在所有在临界区的线程都已追上当前 epoch 时推进一个 epoch，
// 并把满足两个 epoch 隔离的垃圾回收掉。返回是否发生了推进。
func (e *epochMgr) tryAdvance() bool {
	cur := e.current.Load()
	for i := range e.ann {
		a := e.ann[i].Load()
		if a != 0 && a < cur {
			return false
		}
	}
	if !e.current.CompareAndSwap(cur, cur+1) {
		return false
	}
	newCur := cur + 1
	// 排空当前可回收项；不满足的重新塞回队尾。
	pending := make([]limboItem, 0)
	drained := false
	for !drained {
		select {
		case item := <-e.limbo:
			if item.retireEpoch+2 <= newCur {
				e.reclaim(item)
			} else {
				pending = append(pending, item)
			}
	default:
		drained = true
	}
	}
	for _, item := range pending {
		e.limbo <- item
	}
	return true
}

// drainAll 供关闭/测试使用，尝试把 limbo 全部回收（多推进几个 epoch）。
func (e *epochMgr) drainAll() {
	for i := 0; i < 6; i++ {
		e.tryAdvance()
	}
}
