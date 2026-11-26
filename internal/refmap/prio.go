package refmap

import "sync/atomic"

// 确定性伪随机优先级（treap 仅需要比较，不需要密码学随机）。
var prioCounter atomic.Uint64

func nextPrio() uint64 {
	x := prioCounter.Add(0x9e3779b97f4a7c15)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
