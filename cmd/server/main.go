// 演示服务：持续跑后台混合负载，并暴露索引内部结构页面。
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"bwtea/pkg/bwtree"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP 监听地址")
	workers := flag.Int("workers", 24, "后台负载线程数")
	flag.Parse()

	tree := bwtree.New()
	var stop atomic.Bool

	// 后台多线程混合写入，制造真实的增量链、分裂、合并与重试。
	for w := 0; w < *workers; w++ {
		go func(wid int) {
			a := uint64(time.Now().UnixNano()) + uint64(wid)*0x9e3779b97f4a7c15
			b := a ^ 0xd1b54a32d192ed03
			if b == 0 {
				b = 1
			}
			next := func() uint64 {
				x, y := a, b
				a = y
				x ^= x << 23
				b = x ^ y ^ (x >> 17) ^ (y >> 26)
				return b + y
			}
			for !stop.Load() {
				k := bwtree.Key(next() % 5000)
				v := bwtree.Value{BlendRatio: float64(next()%1000) / 10, Tasting: int32(next() % 100)}
				roll := next() % 100
				switch {
				case roll < 15:
					tree.Delete(uint32(wid), k)
				case roll < 45:
					tree.BlindPut(uint32(wid), k, v)
				case roll < 80:
					tree.Put(uint32(wid), k, v)
				default:
					tree.Get(uint32(wid), k)
				}
				if next()%50 == 0 {
					tree.Scan(uint32(wid), k, k+32)
				}
			}
		}(w)
	}

	mux := http.NewServeMux()
	mux.Handle("/debug/bwtree", tree)
	mux.Handle("/debug/bwtree/json", tree)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/debug/bwtree", http.StatusFound)
	})
	fmt.Printf("索引内部结构页面: http://localhost%s/debug/bwtree （2 秒自动刷新）\n", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
