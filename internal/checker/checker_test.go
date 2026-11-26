package checker_test

import (
	"testing"

	"bwtea/internal/checker"
	"bwtea/pkg/bwtree"
)

func seqRec(i int64, wid uint32, kind checker.OpKind, k bwtree.Key, start, end int64, val int64) checker.Rec {
	r := checker.Rec{Seq: i, WID: wid, Kind: kind, Key: k, StartNS: start, EndNS: end}
	if kind == checker.OpPut || kind == checker.OpBlindPut {
		r.Val.WriterSeq = val
	}
	if kind == checker.OpGet {
		r.Found = val >= 0
		if r.Found {
			r.Got.WriterSeq = val
		}
	}
	return r
}

func TestCheckLegal(t *testing.T) {
	// 三个并发写 + 读，全部能排成合法顺序。
	recs := []checker.Rec{
		seqRec(1, 0, checker.OpPut, 10, 1, 10, 100),
		seqRec(2, 1, checker.OpPut, 10, 2, 11, 101),
		seqRec(3, 2, checker.OpGet, 10, 5, 8, 101), // 与两个写并发，读到 101 可线性化
		seqRec(4, 0, checker.OpDelete, 10, 12, 15, 0),
		seqRec(5, 1, checker.OpGet, 10, 16, 18, -1), // 删除之后的读必须是墓碑
	}
	res, err := checker.Check(recs)
	if err != nil {
		t.Fatalf("合法历史被判失败: %v (%s)", err, res.FailureReason)
	}
}

func TestCheckIllegal(t *testing.T) {
	// 写在 [1,5]，读在 [6,8] 却返回了从未写入的值，无法线性化。
	recs := []checker.Rec{
		seqRec(1, 0, checker.OpPut, 10, 1, 5, 100),
		seqRec(2, 1, checker.OpGet, 10, 6, 8, 999),
	}
	if _, err := checker.Check(recs); err == nil {
		t.Fatal("非法历史应被判失败")
	}
}

func TestCheckRealtimeOrder(t *testing.T) {
	// 写 A 完全先于写 B；读发生在 B 之后却返回 A 的值，违反实时顺序。
	recs := []checker.Rec{
		seqRec(1, 0, checker.OpPut, 10, 1, 4, 100),
		seqRec(2, 1, checker.OpPut, 10, 5, 8, 101),
		seqRec(3, 2, checker.OpGet, 10, 9, 12, 100),
	}
	if _, err := checker.Check(recs); err == nil {
		t.Fatal("违反实时顺序的历史应被判失败")
	}
}

func TestCheckBlindAndDelete(t *testing.T) {
	recs := []checker.Rec{
		seqRec(1, 0, checker.OpBlindPut, 7, 1, 4, 1),
		seqRec(2, 0, checker.OpDelete, 7, 5, 8, 0),
		seqRec(3, 1, checker.OpBlindPut, 7, 9, 12, 2),
		seqRec(4, 2, checker.OpGet, 7, 13, 15, 2),
	}
	if _, err := checker.Check(recs); err != nil {
		t.Fatalf("盲写/删除历史应合法: %v", err)
	}
}
