package diyredis

import "testing"

func BenchmarkBulkStr(b *testing.B) {
	for range b.N {
		MakeBulkStr("a test string")
	}
}

func BenchmarkMakeArray(b *testing.B) {
	for range b.N {
		MakeArray([]any{"this", "that", "and the other", "more", "even more", "even more items", "look at how many items!!"})
	}
}
