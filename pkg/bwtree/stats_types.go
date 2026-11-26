package bwtree

import "time"

// workerStats 是单个线程的本地冲突重试等计数（只由属主线程自增，无竞争）。
type workerStats struct {
	casRetry     uint64 // 映射表 CAS 失败导致的重试次数
	splitHelp    uint64 // 替别的线程补做分裂第二步的次数
	mergeAbort   uint64 // 合并遇并发分裂而放弃的次数
	consolidate  uint64 // 物化（合并增量链）次数
	scanRestart  uint64 // 扫描中节点失效导致的重启次数
	locRestart   uint64 // 旧指针走到不负责目标键的节点，重定位次数
	epochAdvance uint64 // 本线程推动 epoch 推进的次数
	opsInsert    uint64
	opsBlind     uint64
	opsUpdate    uint64
	opsDelete    uint64
	opsGet       uint64
	opsScan      uint64
}

// TimelineEvent 是分裂/合并时间线上的一个事件。
type TimelineEvent struct {
	Seq       uint64    `json:"seq"`
	At        time.Time `json:"at"`
	Kind      string    `json:"kind"` // split / split-help / merge / merge-abort
	WID       uint32    `json:"wid"`
	Node      PID       `json:"node"`
	Sibling   PID       `json:"sibling"`
	Parent    PID       `json:"parent"`
	SplitKey  Key       `json:"splitKey"`
	Detail    string    `json:"detail"`
}

// PoolStat 是一个线程内存池的快照。
type PoolStat struct {
	WID      uint32 `json:"wid"`
	FreeLen  uint64 `json:"freeLen"`
	Alloc    uint64 `json:"alloc"`
	Reuse    uint64 `json:"reuse"`
}

// WorkerStat 是单线程计数快照。
type WorkerStat struct {
	WID          uint32 `json:"wid"`
	CASRetry     uint64 `json:"casRetry"`
	SplitHelp    uint64 `json:"splitHelp"`
	MergeAbort   uint64 `json:"mergeAbort"`
	Consolidate  uint64 `json:"consolidate"`
	ScanRestart  uint64 `json:"scanRestart"`
	LocRestart   uint64 `json:"locRestart"`
	EpochAdvance uint64 `json:"epochAdvance"`
	Ops          struct {
		Insert uint64 `json:"insert"`
		Blind  uint64 `json:"blind"`
		Update uint64 `json:"update"`
		Delete uint64 `json:"delete"`
		Get    uint64 `json:"get"`
		Scan   uint64 `json:"scan"`
	} `json:"ops"`
}

// NodeChainStat 是一个逻辑节点的增量链长度快照。
type NodeChainStat struct {
	PID  PID  `json:"pid"`
	Kind uint8 `json:"kind"`
	Lo   Key  `json:"lo"`
	Hi   Key  `json:"hi"`
	Len  int  `json:"chainLen"`
}

// Snapshot 是调试页面使用的完整内部结构快照。
type Snapshot struct {
	TakenAt          time.Time `json:"takenAt"`
	RootPID          PID       `json:"rootPid"`
	SegmentTotal     uint64    `json:"segmentTotal"`
	SegmentReturned  uint64    `json:"segmentReturned"`
	SegmentReused    uint64    `json:"segmentReused"`
	LiveNodes        int       `json:"liveNodes"`
	DeltaKindCounts  map[string]uint64 `json:"deltaKindCounts"`
	ChainLen         struct {
		Max  int     `json:"max"`
		Mean float64 `json:"mean"`
		Hist []int   `json:"hist"` // 按 1,2,3-4,5-8,9-16,17-32,33+ 分桶
	} `json:"chainLen"`
	Workers []WorkerStat `json:"workers"`
	Pools   []PoolStat   `json:"pools"`
	Timeline []TimelineEvent `json:"timeline"`
	Nodes    []NodeChainStat `json:"nodes"`
}
