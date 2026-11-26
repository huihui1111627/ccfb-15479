package bwtree_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"bwtea/pkg/bwtree"
)

func TestBasicCRUD(t *testing.T) {
	tr := bwtree.New()
	const wid = uint32(0)
	// 插入量足够大，强制多次叶子分裂与内层节点分裂。
	const n = 2000
	for i := 0; i < n; i++ {
		k := bwtree.Key(i * 7)
		tr.Put(wid, k, bwtree.Value{Tasting: int32(i), WriterSeq: int64(i)})
	}
	for i := 0; i < n; i++ {
		k := bwtree.Key(i * 7)
		v, ok := tr.Get(wid, k)
		if !ok || v.WriterSeq != int64(i) {
			t.Fatalf("键 %d 读取异常: ok=%v seq=%d", k, ok, v.WriterSeq)
		}
	}
	// 更新走更新增量。
	for i := 0; i < n; i += 2 {
		k := bwtree.Key(i * 7)
		tr.Put(wid, k, bwtree.Value{WriterSeq: int64(i) + 1_000_000})
	}
	for i := 0; i < n; i++ {
		k := bwtree.Key(i * 7)
		v, ok := tr.Get(wid, k)
		want := int64(i)
		if i%2 == 0 {
			want += 1_000_000
		}
		if !ok || v.WriterSeq != want {
			t.Fatalf("更新后键 %d 期望 seq=%d 得到 %d", k, want, v.WriterSeq)
		}
	}
	// 删除一半。
	for i := 0; i < n; i += 2 {
		tr.Delete(wid, bwtree.Key(i*7))
	}
	tr.DrainEpoch()
	for i := 0; i < n; i++ {
		k := bwtree.Key(i * 7)
		_, ok := tr.Get(wid, k)
		if i%2 == 0 && ok {
			t.Fatalf("键 %d 应已删除", k)
		}
		if i%2 == 1 && !ok {
			t.Fatalf("键 %d 应仍存在", k)
		}
	}
	// 全量扫描严格有序、无重复。
	all := tr.Scan(wid, 0, bwtree.MaxKey)
	for i := 1; i < len(all); i++ {
		if all[i-1].Key >= all[i].Key {
			t.Fatalf("扫描非严格递增于 %d", i)
		}
	}
	if len(all) != n/2 {
		t.Fatalf("扫描键数期望 %d 得到 %d", n/2, len(all))
	}
}

func TestBlindWrite(t *testing.T) {
	tr := bwtree.New()
	const wid = uint32(1)
	// 盲写一个此前不存在的键，查找必须直接取盲写值。
	tr.BlindPut(wid, 42, bwtree.Value{WriterSeq: 77})
	if v, ok := tr.Get(wid, 42); !ok || v.WriterSeq != 77 {
		t.Fatalf("盲写未生效: ok=%v v=%+v", ok, v)
	}
	// 盲写覆盖已有键。
	tr.Put(wid, 42, bwtree.Value{WriterSeq: 1})
	tr.BlindPut(wid, 42, bwtree.Value{WriterSeq: 2})
	if v, ok := tr.Get(wid, 42); !ok || v.WriterSeq != 2 {
		t.Fatalf("盲写覆盖失败: %+v", v)
	}
	// 盲写后删除，再盲写。
	tr.Delete(wid, 42)
	if _, ok := tr.Get(wid, 42); ok {
		t.Fatal("删除后应不存在")
	}
	tr.BlindPut(wid, 42, bwtree.Value{WriterSeq: 3})
	if v, ok := tr.Get(wid, 42); !ok || v.WriterSeq != 3 {
		t.Fatalf("删除后盲写失败: %+v", v)
	}
}

func TestScanRange(t *testing.T) {
	tr := bwtree.New()
	const wid = uint32(2)
	for i := 0; i < 500; i++ {
		tr.Put(wid, bwtree.Key(i), bwtree.Value{WriterSeq: int64(i)})
	}
	out := tr.Scan(wid, 100, 200)
	if len(out) != 100 {
		t.Fatalf("范围扫描长度期望 100 得到 %d", len(out))
	}
	for i, kv := range out {
		if kv.Key != bwtree.Key(100+i) {
			t.Fatalf("范围扫描内容错位于 %d: %d", i, kv.Key)
		}
	}
	// 空范围。
	if got := tr.Scan(wid, 100, 100); got != nil {
		t.Fatalf("空范围应返回 nil，得到 %d 条", len(got))
	}
}

func TestConcurrentSmoke(t *testing.T) {
	tr := bwtree.New()
	const workers = 16
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < 1500; i++ {
				k := bwtree.Key((wid*31 + i*13) % 800)
				switch i % 5 {
				case 0:
					tr.Delete(uint32(wid), k)
				case 1:
					tr.BlindPut(uint32(wid), k, bwtree.Value{WriterSeq: int64(i), WriterWID: int32(wid)})
				case 2:
					tr.Get(uint32(wid), k)
				case 3:
					tr.Scan(uint32(wid), k, k+10)
				default:
					tr.Put(uint32(wid), k, bwtree.Value{WriterSeq: int64(i), WriterWID: int32(wid)})
				}
			}
		}(w)
	}
	wg.Wait()
	tr.DrainEpoch()
	all := tr.Scan(0, 0, bwtree.MaxKey)
	for i := 1; i < len(all); i++ {
		if all[i-1].Key >= all[i].Key {
			t.Fatalf("并发后扫描无序于 %d", i)
		}
	}
	snap := tr.Snapshot()
	if snap.LiveNodes == 0 {
		t.Fatal("快照无活节点")
	}
	if snap.ChainLen.Max == 0 {
		t.Fatal("链长统计异常")
	}
}

func TestSegmentReuse(t *testing.T) {
	tr := bwtree.New()
	const wid = uint32(3)
	// 制造大量节点后再清空，驱动整段归还与复用。
	for round := 0; round < 3; round++ {
		for i := 0; i < 3000; i++ {
			tr.Put(wid, bwtree.Key(round*100000+i), bwtree.Value{WriterSeq: int64(i)})
		}
		for i := 0; i < 3000; i++ {
			tr.Delete(wid, bwtree.Key(round*100000 + i))
		}
		tr.DrainEpoch()
	}
	snap := tr.Snapshot()
	if snap.SegmentReturned == 0 {
		t.Fatalf("未观察到整段归还: %+v", snap)
	}
	t.Logf("段总数=%d 归还=%d 复用=%d 活节点=%d",
		snap.SegmentTotal, snap.SegmentReturned, snap.SegmentReused, snap.LiveNodes)
}

func TestDebugHandler(t *testing.T) {
	tr := bwtree.New()
	for i := 0; i < 300; i++ {
		tr.Put(0, bwtree.Key(i*3), bwtree.Value{WriterSeq: int64(i)})
	}
	srv := httptest.NewServer(tr)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/debug/bwtree")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("页面状态码 %d", resp.StatusCode)
	}
	jresp, err := http.Get(srv.URL + "/debug/bwtree/json")
	if err != nil {
		t.Fatal(err)
	}
	defer jresp.Body.Close()
	if jresp.StatusCode != http.StatusOK {
		t.Fatalf("JSON 状态码 %d", jresp.StatusCode)
	}
}
