package bwtree

// DrainEpoch 多轮推进 epoch，把 limbo 中的旧记录与废弃 PID 全部回收。
// 仅供测试/停机核对使用（要求没有线程正在临界区）。
func (t *Tree) DrainEpoch() {
	t.epoch.drainAll()
}

// RetryStats 汇总全树 CAS 重试次数与写操作次数（压测统计用）。
func (t *Tree) RetryStats() (retries, writeOps uint64) {
	for w := range t.stats {
		st := &t.stats[w]
		retries += st.casRetry
		writeOps += st.opsInsert + st.opsBlind + st.opsUpdate + st.opsDelete
	}
	return retries, writeOps
}
