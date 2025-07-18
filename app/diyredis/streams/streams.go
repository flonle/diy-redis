// The streams package implements an append-only Radix tree, highly optimized for use with Redis-style
// stream keys (e.g. "123-9876").
package streams

import (
	"errors"
	"sync"
)

const MaxUint64 = ^uint64(0)

type Stream struct {
	root      RxNode // root node
	LastEntry Entry
	// subscribers map[any]chan NewEntryMsg
	// subscribers []chan NewEntryMsg
	subscribers []subscription
	mutex       sync.RWMutex
}

func NewStream() *Stream {
	return &Stream{
		// subscribers: make(map[any]chan NewEntryMsg),
		subscribers: make([]subscription),
	}
}

type subscription struct {
	id any
	ch chan NewEntryMsg
}

type NewEntryMsg struct {
	Entry
	SubscriptionID any
}

// Append an entry to the stream.
func (s *Stream) Put(key Key, val any) error {
	if key.IsMin() || !key.GreaterThan(s.LastEntry.Key) {
		return errors.New("key too low")
	}

	internalKey := key.internalRepr()

	s.mutex.Lock()

	newNode := s.root.create(internalKey)
	if newNode.entry == nil {
		newNode.entry = &Entry{Key: key, Val: val}
	} else {
		newNode.entry.Key = key
		newNode.entry.Val = val
	}
	s.LastEntry = *newNode.entry

	s.mutex.Unlock()

	// Send new entry to all subscribers (non-blocking, if we can't send we ignore the subscription)
	go func() {
		for id, ch := range s.subscribers {
			select {
			case ch <- NewEntryMsg{SubscriptionID: id, Entry: *newNode.entry}:
			default:
			}
		}
	}()

	return nil
}

// Get the value for a given key, and whether it was found.
func (s *Stream) Search(key Key) (any, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

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

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Optimized case: "since"-like query
	if toKey.IsMax() {
		return s.root.higherEntries(fromKey.internalRepr())
	}

	return s.root.rangeEntries(fromKey.internalRepr(), toKey.internalRepr())
}

// Subscribe to this stream, receiving any newly added entries over the channel ch
// as they come in. The caller MUST unsubcribe sometime later using Unsubscribe().
func (s *Stream) Subscribe(ch chan NewEntryMsg, id any) {
	sub := subscription{id: id, ch: ch}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Add channel to subscribers
	for i, sub := range s.subscribers {
		if sub.ch == nil {
			s.subscribers[i] = sub
			return
		}
	}
	s.subscribers = append(s.subscribers, sub)

	// // Create unsubscribe function
	// unsub = func() {
	// 	s.mutex.Lock()
	// 	defer s.mutex.Unlock()

	// 	for id, ch := range s.subscribers {
	// 		if sub == ch {
	// 			s.subscribers[i] = nil
	// 			return
	// 		}
	// 	}
	// }

	// return unsub
}

func (s *Stream) Unsubscribe(ch chan NewEntryMsg) {
	if ch == nil {
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i, sub := range s.subscribers {
		if sub.ch == ch {
			s.subscribers[i] = subscription{}
		}
	}
}

// func (s *Stream) Unsubscribe(subscriptionID uint) {
// 	if subscriptionID > uint(len(s.subscribers)) {
// 		return
// 	}

// 	s.mutex.Lock()
// 	s.subscribers[subscriptionID] = nil
// 	s.mutex.Unlock()
// }

// Block the goroutine until a new entry is appended to the stream, and return it.
func (s *Stream) WaitForEntry() Entry {
	// TODO: this is an awfully shallow abstraction, despite its clean semantics. Perhaps don't bother.
	ch := make(chan NewEntryMsg)
	subID := s.Subscribe(ch, a)
	defer s.Unsubscribe(subID)

	res := <-ch
	return res.Entry
}
