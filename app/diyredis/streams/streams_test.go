package streams

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	radix "github.com/armon/go-radix"
	anothertrie "github.com/dghubble/trie"
)

const numericRunes = "0123456789"

var testStreamKeys []string

func TestMain(m *testing.M) {
	keys, seed := genRandStreamKeys(1000000)
	fmt.Println("Used seed: ", seed)
	testStreamKeys = keys
	m.Run()
}

// Generate and return `count` pseudo-random StreamKeys,
// alongside the seed used to generate them.
func genRandStreamKeys(count int) ([]string, int64) {
	// seed := rand.Int63()
	var seed int64 = 69
	randgen := rand.New(rand.NewSource(seed))

	streamKeys := make([]string, count)
	var sb strings.Builder
	var wordLen int
	for i := range count {
		wordLen = randgen.Intn(20) + 1 // between 1 and 20 inclusive
		for range wordLen {
			sb.WriteByte(numericRunes[randgen.Intn(len(numericRunes))])
		}
		sb.WriteRune('-')
		wordLen = randgen.Intn(20) + 1 // between 1 and 20 inclusive
		for range wordLen {
			sb.WriteByte(numericRunes[randgen.Intn(len(numericRunes))])
		}

		// streamKeys[i] = StreamEntryKey(sb.String())
		streamKeys[i] = sb.String()
		sb.Reset()
	}

	return streamKeys, seed
}

func TestStreamSetAndTest(t *testing.T) {
	stream := Stream{}

	for i := range 1000 {
		t.Log(testStreamKeys[i])
		stream.Insert(testStreamKeys[i], i)
		got, ok, _ := stream.Search(testStreamKeys[i])
		if !ok {
			t.Errorf("could not find key %v after insertion", testStreamKeys[i])
		}
		if got != i {
			t.Errorf("got %v, want %v", got, i)
		}
	}
}

func TestTrieNotFound(t *testing.T) {
	stream := Stream{}

	for i := range 1000 {
		t.Log(testStreamKeys[i])
		_, ok, _ := stream.Search(testStreamKeys[i])
		if ok {
			t.Errorf("found key %v, but it's not in the trie", testStreamKeys[i])
		}
	}
}

func TestTrieMapCmp(t *testing.T) {
	stream := Stream{}
	cmpMap := map[string]any{}

	for i := range 1000 {
		stream.Insert(testStreamKeys[i], i)
		cmpMap[string(testStreamKeys[i])] = i
	}
	for i := range 1000 {
		got, _, _ := stream.Search(testStreamKeys[i])
		want := cmpMap[string(testStreamKeys[i])]
		if got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestTrieSearchHigher(t *testing.T) {
	stream := Stream{}
	keys := []RxLeafInfo{ // These are ordered from smallest to largest keys
		RxLeafInfo{"1-1", 0},
		RxLeafInfo{"1-2", 0},
		RxLeafInfo{"1-999999999", 0},
		RxLeafInfo{"22-22", 0},
		RxLeafInfo{"69-420", 0},
		RxLeafInfo{"9999-9", 0},
		RxLeafInfo{"9999-10", 0},
		RxLeafInfo{"10000-0", 0},
		RxLeafInfo{"10000-99999999", 0},
		RxLeafInfo{"9999999-9999999", 0},
		RxLeafInfo{"9999999-99999999", 0},
	}
	for _, leafInfo := range keys {
		stream.Insert(leafInfo.Key, leafInfo.Val)
	}

	var res []RxLeafInfo

	// Key does not exist, which should be OK, and is smaller than all inserted keys,
	// so it should return everything
	res, _ = stream.SearchHigher("0-0")
	if !isEqual(keys, res) {
		t.Errorf("got %v, want %v (key %s)", res, keys, "0-0")
	}

	// Test for every key in `keys` that we can successfully find all higher keys,
	// which should be all keys after it
	for i := range len(keys) {
		res, _ = stream.SearchHigher(keys[i].Key)
		if !isEqual(keys[i+1:], res) {
			t.Errorf("got %v, want %v (key %s)", res, keys[i+1:], keys[i].Key)
		}
	}

	// Test SearchHigher with keys that don't exist in the trie
	res, _ = stream.SearchHigher("1-3")
	if !isEqual(keys[2:], res) {
		t.Errorf("got %v, want %v (key %s)", res, keys[2:], "1-3")
	}
	res, _ = stream.SearchHigher("9999-15")
	if !isEqual(keys[7:], res) {
		t.Errorf("got %v, want %v (key %s)", res, keys[2:], "9999-15")
	}
	res, _ = stream.SearchHigher("9999999-0000001")
	if !isEqual(keys[9:], res) {
		t.Errorf("got %v, want %v (key %s)", res, keys[2:], "9999999-0000001")
	}
	res, _ = stream.SearchHigher("10000000-0")
	if !isEqual([]RxLeafInfo{}, res) {
		t.Errorf("got %v, want %v (key %s)", res, keys[2:], "9999999-0000001")
	}
}

func isEqual(first []RxLeafInfo, second []RxLeafInfo) bool {
	if len(first) != len(second) {
		return false
	}

	for i := range len(first) {
		if first[i] != second[i] {
			return false
		}
	}

	return true
}

func BenchmarkTrieInsert(b *testing.B) {
	stream := Stream{}
	b.ResetTimer()
	for i := range b.N {
		stream.Insert(testStreamKeys[i%len(testStreamKeys)], "mycoolval")
	}
}

func BenchmarkTrieSearch(b *testing.B) {
	stream := Stream{}
	for i := range b.N {
		stream.Insert(testStreamKeys[i%len(testStreamKeys)], "mycoolval")
	}
	b.ResetTimer()

	for i := range b.N {
		stream.Search(testStreamKeys[i%len(testStreamKeys)])
	}
}

// func BenchmarkGoMapInsert(b *testing.B) {
// 	mapje := map[string]string{}
// 	b.ResetTimer()
// 	for i := range b.N {
// 		mapje[string(testStreamKeys[i%len(testStreamKeys)])] = "mycoolval"
// 	}
// }

// func BenchmarkGoMapSearch(b *testing.B) {
// 	mapje := map[string]string{}
// 	for i := range b.N {
// 		mapje[string(testStreamKeys[i%len(testStreamKeys)])] = "mycoolval"
// 	}
// 	b.ResetTimer()

// 	for i := range b.N {
// 		_ = mapje[string(testStreamKeys[i%len(testStreamKeys)])]
// 	}
// }

// func BenchmarkGoSyncMapInsert(b *testing.B) {
// 	mapje := sync.Map{}
// 	b.ResetTimer()
// 	for i := range b.N {
// 		mapje.Store(string(testStreamKeys[i%len(testStreamKeys)]), "mycoolval")
// 	}
// }

// func BenchmarkGoSyncMapSearch(b *testing.B) {
// 	mapje := sync.Map{}
// 	for i := range b.N {
// 		mapje.Store(string(testStreamKeys[i%len(testStreamKeys)]), "mycoolval")
// 	}
// 	b.ResetTimer()

// 	for i := range b.N {
// 		_, _ = mapje.Load(string(testStreamKeys[i%len(testStreamKeys)]))
// 	}
// }

func BenchmarkAnotherTrieInsert(b *testing.B) {
	trie := anothertrie.RuneTrie{}
	b.ResetTimer()
	for i := range b.N {
		trie.Put(string(testStreamKeys[i%len(testStreamKeys)]), "mycoolval")
	}
}

func BenchmarkAnotherTrieSearch(b *testing.B) {
	trie := anothertrie.RuneTrie{}
	for i := range b.N {
		trie.Put(string(testStreamKeys[i%len(testStreamKeys)]), "mycoolval")
	}
	b.ResetTimer()

	for i := range b.N {
		trie.Get(string(testStreamKeys[i%len(testStreamKeys)]))
	}
}

func BenchmarkAnotherRadixInsert(b *testing.B) {
	rx := radix.New()
	b.ResetTimer()
	for i := range b.N {
		rx.Insert(string(testStreamKeys[i%len(testStreamKeys)]), "mycoolval")
	}
}

func BenchmarkAnotherRadixSearch(b *testing.B) {
	rx := radix.New()
	for i := range b.N {
		rx.Insert(string(testStreamKeys[i%len(testStreamKeys)]), "mycoolval")
	}
	b.ResetTimer()

	for i := range b.N {
		rx.Get(string(testStreamKeys[i%len(testStreamKeys)]))
	}
}

// func BenchmarkHaxmapInsert(b *testing.B) {
// 	hm := haxmap.New[string, string]()
// 	b.ResetTimer()
// 	for i := range b.N {
// 		hm.Set(string(testStreamKeys[i%len(testStreamKeys)]), "mycoolval")
// 	}
// }

// func BenchmarkHaxmapSearch(b *testing.B) {
// 	hm := haxmap.New[string, string]()
// 	for i := range b.N {
// 		hm.Set(string(testStreamKeys[i%len(testStreamKeys)]), "mycoolval")
// 	}
// 	b.ResetTimer()

// 	for i := range b.N {
// 		hm.Get(string(testStreamKeys[i%len(testStreamKeys)]))
// 	}
// }
