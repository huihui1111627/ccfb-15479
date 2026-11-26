package checker_test

import (
	"testing"
	"bwtea/internal/checker"
	"bwtea/pkg/bwtree"
)

func r(seq int64, wid uint32, kind checker.OpKind, k bwtree.Key, s, e int64, val int64) checker.Rec {
	rec := checker.Rec{Seq: seq, WID: wid, Kind: kind, Key: k, StartNS: s, EndNS: e}
	if kind == checker.OpPut { rec.Val.WriterSeq = val }
	if kind == checker.OpGet { rec.Found = val >= 0; if rec.Found { rec.Got.WriterSeq = val } }
	return rec
}

func TestCheckerExhaustive(t *testing.T) {
	// 两个并发写 A(val=1,[1,4]) B(val=2,[2,5])，读 R([6,7]) 在两者之后。
	// R=1 合法(序 A,B,R 中 B=2 不匹配；序 B,A,R 中 A=1 匹配) => R=1 应通过
	if _, err := checker.Check([]checker.Rec{
		r(1,0,checker.OpPut,1,1,4,1),
		r(2,1,checker.OpPut,1,2,5,2),
		r(3,2,checker.OpGet,1,6,7,1),
	}); err != nil { t.Fatalf("R=1 after concurrent writes should be legal: %v", err) }

	// R=2 也合法
	if _, err := checker.Check([]checker.Rec{
		r(1,0,checker.OpPut,1,1,4,1),
		r(2,1,checker.OpPut,1,2,5,2),
		r(3,2,checker.OpGet,1,6,7,2),
	}); err != nil { t.Fatalf("R=2 should be legal: %v", err) }

	// R=99 是从未写入的值，必失败
	if _, err := checker.Check([]checker.Rec{
		r(1,0,checker.OpPut,1,1,4,1),
		r(2,1,checker.OpGet,1,6,7,99),
	}); err == nil { t.Fatal("never-written value must fail") }

	// 同线程程序序：A[1,2]=1, 同线程 B[3,4]=2, 之后读=1 必失败（B 必须在 A 后）
	if _, err := checker.Check([]checker.Rec{
		r(1,0,checker.OpPut,1,1,2,1),
		r(2,0,checker.OpPut,1,3,4,2),
		r(3,1,checker.OpGet,1,5,6,1),
	}); err == nil { t.Fatal("program-order violation should fail") }
	t.Log("checker exhaustive cases OK")
}
