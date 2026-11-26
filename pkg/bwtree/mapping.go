package bwtree

import (
	"sync"
	"sync/atomic"
)

// 映射表按固定大小的段分配。逻辑号用完的段整段归还，归还的段保持槽位不变、
// 清零后挂到空闲段链表，以后开新段时整段复用。
const segmentSize = 256

// mappingSegment 是一个段：PID -> 物理记录指针的原子表。
type mappingSegment struct {
	entries [segmentSize]atomic.Pointer[Record]
	live    atomic.Int64 // 段内仍在使用的逻辑号数量，归零即整段归还
}

// mappingTable 是分段的逻辑号->物理地址映射表。
// segments 指向一个不可变的 *mappingSegment 数组，扩容时整体替换（真正的原子指针 CAS）。
type mappingTable struct {
	segments atomic.Pointer[[]*mappingSegment]

	mu           sync.Mutex // 保护空闲栈与段分配（不在数据读路径上）
	freePIDs     []PID      // 段内零散空闲的逻辑号
	freeSegments []uint64   // 整段归还、可整体复用的段下标（不含段 0）
	segCount     atomic.Uint64
	segReturned  atomic.Uint64
	segReused    atomic.Uint64
}

func newMappingTable() *mappingTable {
	m := &mappingTable{}
	seg0 := &mappingSegment{}
	segs := []*mappingSegment{seg0}
	m.segments.Store(&segs)
	m.segCount.Store(1)
	return m
}

func pidSegment(pid PID) (seg uint64, idx int) {
	v := uint64(pid) - 1 // PID 从 1 开始
	return v / segmentSize, int(v % segmentSize)
}

func makePID(seg, idx uint64) PID {
	return PID(seg*segmentSize + idx + 1)
}

// load 取逻辑号当前的物理记录。
func (m *mappingTable) load(pid PID) *Record {
	segs := m.segments.Load()
	seg, idx := pidSegment(pid)
	if seg >= uint64(len(*segs)) || (*segs)[seg] == nil {
		return nil
	}
	return (*segs)[seg].entries[idx].Load()
}

// cas 用一次比较并交换把映射表项从 old 换成 new。
func (m *mappingTable) cas(pid PID, old, new *Record) bool {
	segs := m.segments.Load()
	seg, idx := pidSegment(pid)
	if seg >= uint64(len(*segs)) || (*segs)[seg] == nil {
		return false
	}
	return (*segs)[seg].entries[idx].CompareAndSwap(old, new)
}

// allocPID 分配一个逻辑号并登记初始物理记录。
// 顺序：零散空闲号 -> 整段复用 -> 扩容新段。
func (m *mappingTable) allocPID(head *Record) PID {
	for {
		m.mu.Lock()
		if n := len(m.freePIDs); n > 0 {
			pid := m.freePIDs[n-1]
			m.freePIDs = m.freePIDs[:n-1]
			m.mu.Unlock()
			seg, idx := pidSegment(pid)
			s := (*m.segments.Load())[seg]
			s.live.Add(1)
			s.entries[idx].Store(head)
			return pid
		}
		if n := len(m.freeSegments); n > 0 {
			seg := m.freeSegments[n-1]
			m.freeSegments = m.freeSegments[:n-1]
			m.segReused.Add(1)
			m.mu.Unlock()
			pid := makePID(seg, 0)
			s := (*m.segments.Load())[seg]
			s.live.Store(1)
			s.entries[0].Store(head)
			return pid
		}
		oldPtr := m.segments.Load()
		oldSegs := *oldPtr
		newSegs := make([]*mappingSegment, len(oldSegs)+1)
		copy(newSegs, oldSegs)
		newSeg := &mappingSegment{}
		newSegs[len(oldSegs)] = newSeg
		swapped := m.segments.CompareAndSwap(oldPtr, &newSegs)
		m.mu.Unlock()
		if !swapped {
			// 并发扩容：成功者已提交，重读后下一轮在新表上分配。
			continue
		}
		m.segCount.Store(uint64(len(newSegs)))
		pid := makePID(uint64(len(oldSegs)), 0)
		newSeg.live.Store(1)
		newSeg.entries[0].Store(head)
		return pid
	}
}

// freePID 在 epoch 确认安全后释放逻辑号；若整段归零则整段归还。
func (m *mappingTable) freePID(pid PID) {
	seg, idx := pidSegment(pid)
	s := (*m.segments.Load())[seg]
	s.entries[idx].Store(nil)
	if s.live.Add(-1) != 0 {
		m.mu.Lock()
		m.freePIDs = append(m.freePIDs, pid)
		m.mu.Unlock()
		return
	}

	// 整段用完：清零并挂到空闲段链表，供以后整段复用。段 0 保留。
	m.mu.Lock()
	if s.live.Load() == 0 {
		for i := range s.entries {
			s.entries[i].Store(nil)
		}
		m.segReturned.Add(1)
		if seg != 0 {
			m.freeSegments = append(m.freeSegments, seg)
		}
	}
	m.mu.Unlock()
}

// segmentStats 返回 (当前段槽位数, 累计整段归还次数, 累计整段复用次数)。
func (m *mappingTable) segmentStats() (total, returned, reused uint64) {
	return m.segCount.Load(), m.segReturned.Load(), m.segReused.Load()
}
