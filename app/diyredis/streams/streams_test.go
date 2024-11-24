package streams

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	radix "github.com/armon/go-radix"
	anothertrie "github.com/dghubble/trie"
)

var testStreamKeys []Key
var seed int64

func TestMain(m *testing.M) {
	seed = rand.Int63()
	fmt.Println("Using seed", seed)
	testStreamKeys = genRandStreamKeys(seed, 10000)
	m.Run()
}

// Generate and return `count` pseudo-random StreamKeys,
// alongside the seed used to generate them.
func genRandStreamKeys(seed int64, count int) []Key {
	randgen := rand.New(rand.NewSource(seed))

	streamKeys := make([]Key, count)
	for i := range count {
		streamKeys[i] = Key{randgen.Uint64(), randgen.Uint64()}
	}

	// sort low to high
	sort.Slice(streamKeys, func(i, j int) bool {
		return streamKeys[i].LesserThan(streamKeys[j])
	})

	return streamKeys
}

func TestKeyGenBasic(t *testing.T) {
	internalReprDiff := func(val1 []uint8, val2 []uint8) bool {
		if len(val1) != len(val2) {
			return true
		}
		for i, v := range val1 {
			if v != val2[i] {
				return true
			}
		}
		return false
	}

	stream := Stream{}
	key1 := Key{0, 0}
	key1internalRepr := key1.internalRepr()
	if len(key1internalRepr) != 22 || key1.LeftNr != 0 || key1.RightNr != 0 || internalReprDiff(key1internalRepr, []uint8{21: 0}) {
		t.Errorf("wrong key generated for number 0, 0")
	}

	// Check equality of behavior
	for i := range 1000 {
		keyFromInt := testStreamKeys[i]
		keyFromStr, err := NewKey(keyFromInt.String(), stream)
		if err != nil {
			t.Errorf("got error during test: %v", err)
		}

		keyMismatch := internalReprDiff(keyFromInt.internalRepr(), keyFromStr.internalRepr()) ||
			keyFromInt.LeftNr != keyFromStr.LeftNr ||
			keyFromInt.RightNr != keyFromStr.RightNr
		if keyMismatch {
			t.Error("mismatch between key made from integers and key made from string")
		}
	}
	key2, err := NewKey("0-0", stream)
	if err != nil {
		t.Errorf("got error during test: %v", err)
	}
	if key1.LeftNr != key2.LeftNr || key1.RightNr != key2.RightNr || internalReprDiff(key1.internalRepr(), key2.internalRepr()) {
		t.Error("mismatch between key made from integers and key made from string")
	}

	// Check the base64 internal representation
	if internalReprDiff(Key{0, 63}.internalRepr(), []uint8{21: 63}) {
		t.Errorf("wrong internal representation of key (%v,%v)", 0, 63)
	}
	if internalReprDiff(Key{0, 64}.internalRepr(), []uint8{20: 1, 21: 0}) {
		t.Errorf("wrong internal representation of key (%v, %v)", 0, 64)
	}
	if internalReprDiff(Key{0, 127}.internalRepr(), []uint8{20: 1, 21: 63}) {
		t.Errorf("wrong internal representation of key (%v, %v)", 0, 127)
	}
	if internalReprDiff(Key{0, 128}.internalRepr(), []uint8{20: 2, 21: 0}) {
		t.Errorf("wrong internal representation of key (%v, %v)", 0, 128)
	}
}

func TestKeyGenWildcard(t *testing.T) {
	stream := Stream{}

	key1, err := NewKey("5-5", stream)
	if err != nil {
		t.Errorf("got error while creating new key: %v", err)
	}
	err = stream.Put(key1, 3)
	if err != nil {
		t.Errorf("got error while inserting key: %v", err)
	}

	key2, err := NewKey("5-*", stream)
	if err != nil {
		t.Errorf("got error while creating new key: %v", err)
	}
	if key2.LeftNr != 5 || key2.RightNr != 6 {
		t.Errorf("wrong key value for partial wildcard: %v", key2)
	}

	key3, err := NewKey("*", stream)
	if err != nil {
		t.Errorf("got error while creating new key: %v", err)
	}
	if key3.LeftNr == 0 || key3.RightNr != 0 {
		t.Errorf("wrong key value for wildcard on empty stream: %v", key3)
	}
	stream.Put(key3, 1)

	key4, err := NewKey("*", stream)
	if err != nil {
		t.Errorf("got error while creating new key: %v", err)
	}
	if !key4.GreaterThan(key3) {
		t.Errorf("wilcard key value not larger than previous insert (key %v)", key4)
	}

	// Try inserting a key that is smaller than the last insertion
	err = stream.Put(key1, 0)
	if err == nil {
		t.Errorf("a key smaller than the last was inserted without error")
	}
}

func TestStreamSetAndTest(t *testing.T) {
	stream := Stream{}

	for i := range 1000 {
		key := testStreamKeys[i]
		err := stream.Put(key, i)
		if err != nil {
			t.Errorf("got error while inserting key %s: %s", key, err)
		}
		got, ok := stream.Search(key)
		if !ok {
			t.Errorf("could not find key %v after insertion", key)
			t.Log(i)
			continue
		}
		if got != i {
			t.Errorf("got %v, want %v", got, i)
		}
	}
}

func TestTrieNotFound(t *testing.T) {
	stream := Stream{}

	for i := range 1000 {
		_, ok := stream.Search(testStreamKeys[i])
		if ok {
			t.Errorf("key %v is not in the stream", testStreamKeys[i])
		}
	}
}

func TestTrieMapCmp(t *testing.T) {
	stream := Stream{}
	cmpMap := map[Key]any{}

	for i := range 1000 {
		stream.Put(testStreamKeys[i], i)
		cmpMap[testStreamKeys[i]] = i
	}

	for i := range 1000 {
		got, _ := stream.Search(testStreamKeys[i])
		want := cmpMap[testStreamKeys[i]]
		if got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestRangeHigherThan(t *testing.T) {
	stream := Stream{}
	keys := []Entry{ // These are ordered from smallest to largest keys
		{Key{1, 1}, 0},
		{Key{1, 2}, 0},
		{Key{1, 999999999}, 0},
		{Key{22, 22}, 0},
		{Key{69, 420}, 0},
		{Key{9999, 9}, 0},
		{Key{9999, 10}, 0},
		{Key{10000, 0}, 0},
		{Key{10000, 99999999}, 0},
		{Key{9999999, 9999999}, 0},
		{Key{9999999, 99999999}, 0},
	}
	for _, leafInfo := range keys {
		stream.Put(leafInfo.Key, leafInfo.Val)
	}

	var res []Entry

	// Key does not exist, which should be OK, and is smaller than all inserted keys,
	// so it should return everything
	res = stream.Range(MinKey, MaxKey)
	if !isEqual(keys, res) {
		t.Errorf("got %v, want %v (key %s)", res, keys, "0-0")
	}

	// Test for every key in `keys` that we can successfully find all higher keys,
	// which should be all keys after it
	for i := range len(keys) {
		res = stream.Range(keys[i].Key, MaxKey)
		if !isEqual(keys[i:], res) {
			t.Errorf("got %v, want %v (key %s)", res, keys[i+1:], keys[i].Key)
		}
	}

	// Test SearchHigher with keys that don't exist in the trie
	res = stream.Range(Key{1, 3}, MaxKey)
	if !isEqual(keys[2:], res) {
		t.Errorf("got %v, want %v (key %s)", res, keys[2:], "1-3")
	}
	res = stream.Range(Key{9999, 15}, MaxKey)
	if !isEqual(keys[7:], res) {
		t.Errorf("got %v, want %v (key %s)", res, keys[2:], "9999-15")
	}
	res = stream.Range(Key{9999999, 0000001}, MaxKey)
	if !isEqual(keys[9:], res) {
		t.Errorf("got %v, want %v (key %s)", res, keys[2:], "9999999-0000001")
	}
	res = stream.Range(Key{10000000, 0}, MaxKey)
	if !isEqual([]Entry{}, res) {
		t.Errorf("got %v, want %v (key %s)", res, keys[2:], "9999999-0000001")
	}
}

func TestRangeComplex(t *testing.T) {
	stream := Stream{}
	for i, key := range testStreamKeys {
		stream.Put(key, i)
	}

	randgen := rand.New(rand.NewSource(seed))
	for range 100 {
		fromKey := Key{randgen.Uint64(), randgen.Uint64()}
		toKey := Key{randgen.Uint64(), randgen.Uint64()}
		for _, entry := range stream.Range(fromKey, toKey) {
			if entry.Key.LesserThan(fromKey) || entry.Key.GreaterThan(toKey) {
				t.Errorf(
					"entry in Range() resultset has key %s, which is not between %s and %s",
					entry.Key, fromKey, toKey,
				)
				return
			}
		}
	}
}

func isEqual(first []Entry, second []Entry) bool {
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
		key := testStreamKeys[i%len(testStreamKeys)]
		stream.Put(key, "mycoolval")
	}
}

func BenchmarkTrieSearch(b *testing.B) {
	stream := Stream{}
	for i := range b.N {
		key := testStreamKeys[i%len(testStreamKeys)]
		stream.Put(key, "mycoolval")
	}
	b.ResetTimer()

	for i := range b.N {
		key := testStreamKeys[i%len(testStreamKeys)]
		stream.Search(key)
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
		trie.Put(testStreamKeys[i%len(testStreamKeys)].String(), "mycoolval")
	}
}

func BenchmarkAnotherTrieSearch(b *testing.B) {
	trie := anothertrie.RuneTrie{}
	for i := range b.N {
		trie.Put(testStreamKeys[i%len(testStreamKeys)].String(), "mycoolval")
	}
	b.ResetTimer()

	for i := range b.N {
		trie.Get(testStreamKeys[i%len(testStreamKeys)].String())
	}
}

func BenchmarkAnotherRadixInsert(b *testing.B) {
	rx := radix.New()
	b.ResetTimer()
	for i := range b.N {
		rx.Insert(testStreamKeys[i%len(testStreamKeys)].String(), "mycoolval")
	}
}

func BenchmarkAnotherRadixSearch(b *testing.B) {
	rx := radix.New()
	for i := range b.N {
		rx.Insert(testStreamKeys[i%len(testStreamKeys)].String(), "mycoolval")
	}
	b.ResetTimer()

	for i := range b.N {
		rx.Get(testStreamKeys[i%len(testStreamKeys)].String())
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
