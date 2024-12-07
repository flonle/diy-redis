// The streams package implements an append-only Radix tree, highly optimized for use with Redis-style
// stream keys (e.g. "123-9876").
package streams

import "errors"

const MaxUint64 = ^uint64(0)

type Stream struct {
	root      RxNode // root node
	LastEntry Entry
}

// Append an entry to the stream.
func (s *Stream) Put(key Key, val any) error {
	if key.IsMin() || !key.GreaterThan(s.LastEntry.Key) {
		return errors.New("key too low")
	}

	newNode := s.root.create(key.internalRepr())
	if newNode.entry == nil {
		newNode.entry = &Entry{Key: key, Val: val}
	} else {
		newNode.entry.Key = key
		newNode.entry.Val = val
	}
	s.LastEntry = *newNode.entry
	return nil
}

// Get the value for a given key, and whether it was found.
func (s *Stream) Search(key Key) (any, bool) {
	node, failIdx, _ := s.root.longestCommonPrefix(key.internalRepr())
	if failIdx == -1 {
		return node.entry.Val, true
	} else {
		return nil, false
	}
}

// Get all entries between the two given keys, inclusively.
// Results are ordered from lowest to highest key.
//
// If fromKey > toKey; the resultset will be empty.
func (s *Stream) Range(fromKey Key, toKey Key) []Entry {
	if !fromKey.LesserThan(toKey) {
		return []Entry{}
	}

	// Optimized case: "since"-like query
	if toKey.IsMax() {
		return s.root.higherEntries(fromKey.internalRepr())
	}

	return s.root.rangeEntries(fromKey.internalRepr(), toKey.internalRepr())
}
