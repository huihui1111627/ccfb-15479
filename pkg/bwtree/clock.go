package bwtree

import "time"

// now 独立成函数便于测试替换（当前直接取墙钟）。
func now() time.Time { return time.Now() }
