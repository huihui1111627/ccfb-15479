// Package bwtree 实现一个不加锁的有序索引（Bw-tree 风格）。
//
// 核心约定：
//   - 所有节点只通过“逻辑号(PID) -> 物理记录(*Record)”的分段映射表访问；
//   - 节点从不原地修改，任何改动都生成一条增量记录挂在增量链前面，
//     再用一次 CAS 替换映射表项，CAS 失败则重读重来；
//   - 增量记录为固定大小，从各线程自己的内存池分配，失败的增量直接退回本池。
package bwtree

// Key 是茶样编号，有序索引的键。
type Key uint64

// MaxKey 是键空间上界（根节点范围的排他上界）。
const MaxKey Key = ^Key(0)

// Value 是一份配方上的配比与试饮评分等业务数据。
type Value struct {
	BlendRatio float64 // 配比
	Tasting    int32   // 试饮评分
	WriterSeq  int64   // 写入序号（调试/核对用）
	WriterWID  int32   // 写入线程（调试/核对用）
}

// KV 是扫描返回的键值对。
type KV struct {
	Key Key
	Val Value
}

// PID 是节点逻辑号（Page ID），0 保留为无效号。
type PID uint64

const invalidPID PID = 0

// 增量/基础记录类型。
const (
	recLeafBase     uint8 = 1 // 叶子基础记录
	recInternalBase uint8 = 2 // 内部节点基础记录
	recInsert       uint8 = 3 // 插入增量
	recUpdate       uint8 = 4 // 更新增量
	recDelete       uint8 = 5 // 删除增量
	recBlindWrite   uint8 = 6 // 盲写增量（不查旧值的 upsert）
	recSplit        uint8 = 7 // 分裂增量（指向新兄弟节点）
	recMerge        uint8 = 8 // 合并增量（本节点将并入左兄弟）
	recAbortMerge   uint8 = 9 // 放弃合并增量
	recIndexEntry   uint8 = 10 // 内部节点：新增路由项增量（携带子节点键范围）
	recRemoveKey    uint8 = 11 // 内部节点：删除分隔键增量
)

// kindCount 是类型计数数组长度（含下标 0 占位）。
const kindCount = 12

// deltaKindName 供统计页展示。
var deltaKindName = map[uint8]string{
	recLeafBase:     "叶子基础记录",
	recInternalBase: "内部基础记录",
	recInsert:       "插入增量",
	recUpdate:       "更新增量",
	recDelete:       "删除增量",
	recBlindWrite:   "盲写增量",
	recSplit:        "分裂增量",
	recMerge:        "合并增量",
	recAbortMerge:   "放弃合并增量",
	recIndexEntry:   "路由项增量",
	recRemoveKey:    "分隔键删除增量",
}

// Record 是固定大小的物理记录。
// 增量记录通过 next 指向旧物理记录（物理地址），形成增量链；
// 基础记录（叶子/内部）的 next 为 nil，负载用单独的切片承载。
type Record struct {
	next *Record

	// 基础节点负载（二选一，仅基础记录使用）。
	lp *leafPayload
	ip *internalPayload

	// 节点负责的键范围 [lo, hi)，基础记录与结构增量都会携带。
	lo Key
	hi Key

	// 叶子数据类增量使用的键值；盲写/插入/更新/删除都靠它定位。
	key Key
	val Value

	// 结构增量字段：
	//   recSplit/recMerge: sib=兄弟节点, parent=父节点, hi=分裂点/分隔键
	//   recIndexEntry/recRemoveKey: child=目标子节点, lo/hi=其负责范围
	sib    PID
	parent PID
	child  PID

	owner uint32 // 分配该记录的线程池编号，epoch 回收时退回原池
	kind  uint8
}

// leafPayload 是叶子基础记录的物化负载。
type leafPayload struct {
	keys []Key
	vals []Value
}

// routeEntry 是内部节点的一条路由项：child 负责 [lo, hi)。
type routeEntry struct {
	child PID
	lo    Key
	hi    Key
}

// internalPayload 是内部节点基础记录的物化负载，按 lo 有序。
type internalPayload struct {
	entries []routeEntry
}

func (r *Record) isBase() bool {
	return r.kind == recLeafBase || r.kind == recInternalBase
}
