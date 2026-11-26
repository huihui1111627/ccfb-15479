package stress

import _ "unsafe"

//go:linkname nanotimeRuntime runtime.nanotime
func nanotimeRuntime() int64

// monoNow 返回 runtime 单调时钟（保证严格单调递增，亚纳秒分辨率），
// 避免 time.Now().UnixNano() 在多核上的相等/回退干扰线性化判定。
func monoNow() int64 { return nanotimeRuntime() }
