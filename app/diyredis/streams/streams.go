// The streams package implements an append-only Radix tree, highly optimized for use with Redis-style
// stream keys (e.g. "123-9876").
//
// Uses a bitwise trie with bitmap, or "Array Mapped Tree" (AMT), but with a twist.
// Array Mapped Tree -> https://infoscience.epfl.ch/server/api/core/bitstreams/607d2e29-f659-463b-b2e0-4b910300d2cf/content
// Additionally, single-child nodes are compressed, making this a Radix.
//
// Each internal node has a bitmap that is used to denote valid child nodes.
// Each bit in the bitmap represents the presence (or absence) of a child node.
//
// Keys have the form "123-6446", and are normalized into an internal key before use.
// This normalization consists of parsing the two base-10 numbers in the key,
// separated by "-", and representing them as base-64 in a slice of integers.
// Each item in the slice is a digit in the base-64 number, between 0 and 63, inclusive.
//
// The two slices have a fixed length of 11 (since you need no more base-64 digits to
// represent all uint64 values). They are concatenated together, yielding a final
// internal "key" that always has a length of 22.
//
// > By zero-padding the internal keys, all values are pushed out to the leaves of the
// > tree. Hence why internal nodes cannot keep values. The primary reasons for this
// > is to allow efficient range operations. A prefix tree with fixed-length keys,
// > denoting numbers in a common base (in this case 64), will maintain the invariant
// > that for any node, all nodes with a "smaller" key will be to the left of it, and
// > all nodes with a "larger" key will be to the right of it.
//
// The field bitmap is used to denote the valid child branches of the node.
// Each bit in the bitmap represents the presence (or absence) of a child node.
// The aforementioned slice of digits functions as bit shift offsets into this bitmap,
// in order to find the bit that signifies the existence of a corresponding child node.
//
// >                           𐄂                                   ✓  ✓𐄂
// >  |--------------------------uint64-bitmap---------------------------|
// >  | 0000000000000000000000000000000000000000000000000000000000111010 |
// >  |------------------------------------------------------------------|
// >                           ↑                                   ↑  ↑↑
// >  ex. shift offset = 41 ---|                                   |  ||
// >  ex. shift offset = 5 ----------------------------------------|  ||
// >  ex. shift offset = 2 -------------------------------------------||
// >  ex. shift offset = 1 --------------------------------------------|
//
// Once the existence of a subnode is determined, a bitwise "population count"
// helps us determine the index into the `children` slice. The total number of high bits
// in the bitmap *before* the bit we just checked, is our index.
// We use the `bits.OnesCount64()` function for this, which is actually an
// "intrinstic function"; the Go compiler will emit a native population count
// instruction whenever the host architecture allows it.
package streams

import (
	"errors"
	"math/bits"
	"strconv"
	"time"
)

type StreamEntryKey struct {
	LeftNr       uint64
	RightNr      uint64
	internalRepr []uint8 // internal representation of the key
}

func (k StreamEntryKey) String() string {
	return strconv.FormatUint(k.LeftNr, 10) + "-" + strconv.FormatUint(k.RightNr, 10)
}

// Return true if k is greater than k2
func (k StreamEntryKey) GreaterThan(k2 StreamEntryKey) bool {
	if k.LeftNr > k2.LeftNr {
		return true
	} else if k.LeftNr == k2.LeftNr && k.RightNr > k2.RightNr {
		return true
	}
	return false
}

func NewKeyFromNumbers(number1 uint64, number2 uint64) StreamEntryKey {
	keyObj := StreamEntryKey{LeftNr: number1, RightNr: number2}

	// transform the two numbers into one byte slice of offets into a node's bitmap
	buf := make([]uint8, 22) // key must always be len 22
	toBase64(buf[:11], number1)
	toBase64(buf[11:], number2)
	keyObj.internalRepr = buf

	return keyObj
}

const MaxUint64 = ^uint64(0)

type Stream struct {
	root    RxNode // root node
	LastKey StreamEntryKey
}

// A Radix tree node.
type RxNode struct {
	bitmap        uint64
	children      []RxNode
	extraPrefixes []uint8     // extra prefixes (internal key symbols) for compressed single-child nodes. Any children of the node belongs to the last symbol in this field.
	leafInfo      *RxLeafInfo // If not nil, indicates that the node is a leaf.
}

// Information specific to RxNodes that are leafs.
type RxLeafInfo struct {
	Key string
	Val any
}

func (s *Stream) Insert(key string, val any) error {
	keyObj, err := s.NewKey(key)
	if err != nil {
		return err
	}

	s.InsertKey(keyObj, val)
	return nil
}

func (s *Stream) InsertKey(key StreamEntryKey, val any) {
	newNode := s.root.create(key.internalRepr)
	if newNode.leafInfo == nil {
		newNode.leafInfo = &RxLeafInfo{Key: key.String(), Val: val}
	} else {
		newNode.leafInfo.Key = key.String()
		newNode.leafInfo.Val = val
	}
	s.LastKey = key
}

func (s *Stream) Search(key string) (any, bool, error) {
	keyObj, err := s.NewKey(key)
	if err != nil {
		return nil, false, err
	}

	val, found := s.SearchKey(keyObj)
	return val, found, nil
}

func (s *Stream) SearchKey(key StreamEntryKey) (any, bool) {
	// Return the value for a given key, and whether it was found
	node, failIdx, _ := s.root.find(key.internalRepr)
	if failIdx == -1 {
		return node.leafInfo.Val, true
	} else {
		return nil, false
	}
}

// func (n *RxNode) Insert(key StreamEntryKey, val any) error {
// 	newNode := n.create(key.internalRepr)
// 	} else {
// 	if newNode.leafInfo == nil {
// 		newNode.leafInfo = &RxLeafInfo{Key: key, Val: val}
// 		newNode.leafInfo.Key = key
// 		newNode.leafInfo.Val = val
// 	}
// 	return nil
// }

func (s *Stream) SearchHigher(key string) ([]RxLeafInfo, error) {
	keyObj, err := s.NewKey(key)
	if err != nil {
		return nil, err
	}

	return s.SearchHigherKey(keyObj), nil
}

// Return all values with a key higher than `key`, ordered from lowest to highest key.
//
// A key is seen as one big number conceived by concatenating the two numbers either
// of the hyphen.
func (s *Stream) SearchHigherKey(key StreamEntryKey) []RxLeafInfo {
	higherNodes := s.root.findHigherNodes(key.internalRepr)
	leaves := make([]RxLeafInfo, 0, len(higherNodes)) // AT LEAST as many leaves as there are nodes
	for i := len(higherNodes) - 1; i >= 0; i-- {
		// Reverse iteration because findHigherNodes returns from largest to smallest
		leaves = append(leaves, higherNodes[i].getAllLeaves()...)
	}
	return leaves
}

// Find the node with the longest common prefix with `key`.
//
// Also returns the index, of `key`, where the search failed. If it never failed,
// this value will be -1 and `bestMatch` is guaranteed to be an *exact* match.
// Additionally, if the search failed while walking the node's `extraPrefixes` field,
// the third return value will contain the failing index of `extraPrefixes`.
// If it did not, this value will also be -1.
//
// > In a 'failed' state, where `failIdx != -1`, `bestMatch` has
// > no valid children (be it compressed or not) for `key[failIdx]`.
//
// > In a 'success' state, where `failIdx == -1`, `bestMatch` is
// > always a leaf node.
func (n *RxNode) find(key []uint8) (bestMatch *RxNode, failIdx int, extraFailIdx int) {
	var currentNode = n
	for i := 0; ; i++ {
		if len(currentNode.extraPrefixes) > 0 {
			// node is compressed, walk extraPrefix instead
			var ii int
			var prefix uint8
			for ii, prefix = range currentNode.extraPrefixes {
				if prefix != key[i+ii] { // this can never be out of bounds because keys are always length 22 and as a result a node's maximum extraPrefix length is (22 - prefix), where prefix is derived from the parent nodes.
					// no match == end of search
					return currentNode, i + ii, ii
				}
			}
			i += ii + 1
		}

		if i == len(key) {
			return currentNode, -1, -1 // we looped over all digits in key, either via `children` or via `extraPrefix`.
			// `i should always be == len(key) here, the first iteration value that is no longer a valid index into key.
			// Because the tree has a constant depth (not considering compression) of len(key),
			// we know we are at the deepest (leaf) node. `currentNode` will never have children here.
		}

		// child is not compressed and should thus be in `children`.
		bitmapOffset := key[i]
		bitmask := uint64(1 << bitmapOffset)
		if currentNode.bitmap&bitmask == 0 { // no valid child
			return currentNode, i, -1
		}
		currentNode = &currentNode.children[getChildIdx(currentNode.bitmap, bitmapOffset)]
	}
}

// Return all nodes with higher value keys that are siblings along the branch that makes
// up `key`.
// Note that this does not return *all* higher nodes -- it just does a DFS for `key`,
// grabbing any sibling nodes with a higher key at every level.
//
// Returns nodes ordered by key, highest to lowest.
func (n *RxNode) findHigherNodes(key []uint8) []*RxNode {
	rightSideNodes := []*RxNode{}
	var currentNode = n
	for i := 0; ; i++ {
		if len(currentNode.extraPrefixes) > 0 {
			// node is compressed, walk extraPrefix instead
			var ii int
			var prefix uint8
			for ii, prefix = range currentNode.extraPrefixes {
				if prefix < key[i+ii] { // this can never be out of bounds because keys are always length 22 and as a result a node's maximum extraPrefix length is (22 - prefix), where prefix is derived from the parent nodes.
					// No keys under this node can ever be higher
					return rightSideNodes
				} else if prefix > key[i+ii] {
					// All keys under this node are guaranteed to be higher
					return append(rightSideNodes, currentNode)
				}
				// If prefix == key[i+ii], we have a match and must continue
				continue
			}
			i += ii + 1
		}

		if i == len(key) {
			return rightSideNodes
		}

		// child is not compressed and should thus be in `children`.
		bitmapOffset := key[i]
		bitmask := uint64(1 << bitmapOffset)
		childIdx := getChildIdx(currentNode.bitmap, bitmapOffset)

		if currentNode.bitmap&bitmask == 0 {
			// child does not exist: take all children higher than the hypothetical child, and return
			return appendPtrsReverse(rightSideNodes, currentNode.children[childIdx:])
		}

		// child exists: take all higher children and continue
		rightSideNodes = appendPtrs(rightSideNodes, currentNode.children[childIdx+1:])
		// Note: higher children are all nodes to the left in the children slice
		currentNode = &currentNode.children[childIdx]
	}
}

// Does the unfortunate job of appending a pointer to each element of `slice`, to
// `ptrSlice`.
func appendPtrs(ptrSlice []*RxNode, slice []RxNode) []*RxNode {
	for _, elem := range slice {
		ptrSlice = append(ptrSlice, &elem)
	}
	return ptrSlice
}

// Does the unfortunate job of appending a pointer to each element of `slice`, to
// `ptrSlice`, in reverse order.
func appendPtrsReverse(ptrSlice []*RxNode, slice []RxNode) []*RxNode {
	for i := (len(slice) - 1); i >= 0; i-- {
		ptrSlice = append(ptrSlice, &slice[i])
	}
	return ptrSlice
}

// Get `RxLeafInfo` of all leaves that are a child of `n`.
// Returns are ordered by key, lowest to highest.
func (n *RxNode) getAllLeaves() []RxLeafInfo {
	leafInfos := make([]RxLeafInfo, 0, 1)

	nodeStack := []*RxNode{n}
	var node *RxNode
	// DFS w/ stack
	for len(nodeStack) > 0 {
		nodeStack, node = pop(nodeStack)
		if node.leafInfo != nil {
			leafInfos = append(leafInfos, *node.leafInfo)
		} else {
			nodeStack = appendPtrsReverse(nodeStack, node.children)
		}
	}

	return leafInfos
}

func pop(s []*RxNode) ([]*RxNode, *RxNode) {
	val := s[len(s)-1]
	return s[:len(s)-1], val
}

// Return a node satisfying `key`, starting from `n`, creating any nodes necessary.
func (n *RxNode) create(key []uint8) *RxNode {
	node, failIdx, extraFailIdx := n.find(key)
	if failIdx == -1 {
		return node // node already exists!
	}

	var newNode *RxNode
	if extraFailIdx == -1 {
		// search failed when it could not find an appropriate child node
		bitmapOffset := key[failIdx] // the search failed to find `failIdx`, so we make a node for it
		bitmask := uint64(1 << bitmapOffset)
		node.bitmap |= bitmask
		childIdx := getChildIdx(node.bitmap, bitmapOffset)
		node.appendChild(childIdx)
		newNode = &node.children[childIdx]
	} else {
		// Search failed while walking `extraPrefixes` -> Split the current compressed
		// node by creating a new copy with ony the `extraPrefixes` after the split
		splitNode := *node // shallow copy
		splitNode.extraPrefixes = node.extraPrefixes[extraFailIdx+1:]
		// ↑ Note that I DO NOT create a new backing array; both `splitNode.extraPrefixes`
		// and `node.extraPrefixes` now share the same backing array.
		// This is not a problem because this is an append-only data structure and,
		// as a result, we never mutate the `extraPrefixes` field. We only ever
		// create it, or split it.
		// We would not be able to do this if, for example, a Remove() method existed.
		// Then, we would have to append to an existing `extraPrefixes` when re-compressing
		// after a deletion.

		// Fix current node by setting `extraPrefixes` to only those before the split,
		// as well as setting its two new children: the split node and a new node
		// for the remaining digits in `key`
		splitNodeOffset := node.extraPrefixes[extraFailIdx]
		newNodeOffset := key[failIdx]
		if newNodeOffset > splitNodeOffset {
			node.children = []RxNode{splitNode, RxNode{}}
			newNode = &node.children[1]
		} else {
			node.children = []RxNode{RxNode{}, splitNode}
			newNode = &node.children[0]
		}
		node.extraPrefixes = node.extraPrefixes[:extraFailIdx]
		node.bitmap = uint64(1 << splitNodeOffset)
		node.bitmap |= uint64(1 << newNodeOffset)
		node.leafInfo = nil
	}

	// If there are any more symbols of `key` that need to be injected into the three,
	// we can compress them all into `newNode`, because we're inserting a single
	// value into the tree so no branches are possible from here to leaf
	lastPartOfKey := key[failIdx+1:]
	if len(lastPartOfKey) > 0 {
		newNode.extraPrefixes = make([]uint8, len(lastPartOfKey))
		copy(newNode.extraPrefixes, lastPartOfKey)
	}

	return newNode
}

// Check `bitmap` against `bitmapOffset` and return what the index of the corresponding
// child node *would* be. Does not check if the child actually exists.
func getChildIdx(bitmap uint64, bitmapOffset uint8) int {
	if bitmapOffset == 0 {
		return 0
	}
	onesCountBitmask := MaxUint64 >> (64 - bitmapOffset)
	return bits.OnesCount64(bitmap & onesCountBitmask)
}

// Make sure `childIdx` is a valid index in `children` of `n`. Will be an empty node.
func (n *RxNode) appendChild(childIdx int) {
	if n.children == nil {
		n.children = []RxNode{RxNode{}}
		return
	}
	// Custom growth factor. This is something that can be tuned: a larger factor will
	// waste more memory but have less allocations, a smaller factor will incur more
	// allocations but be more memory efficient.
	// The default is +2, which leans very heavily toward memory efficiency
	if len(n.children)+1 > cap(n.children) {
		newChildren := make([]RxNode, len(n.children)+1, cap(n.children)+2)
		copy(newChildren, n.children[:childIdx])
		copy(newChildren[childIdx+1:], n.children[childIdx:])
		n.children = newChildren
		return
	}

	// Stretch existing slice for new childIdx
	n.children = n.children[:len(n.children)+1]
	copy(n.children[childIdx+1:], n.children[childIdx:])
	n.children[childIdx] = RxNode{}
}

func (s *Stream) NewKey(key string) (StreamEntryKey, error) {
	part1, part2, err := s.parseStreamKey(key)
	if err != nil {
		return StreamEntryKey{}, err
	}
	return NewKeyFromNumbers(part1, part2), nil
}

// Parse a stream key string, e.g. "123-123", into two integers, e.g. `123` & `123`.
// Streamkeys always denote base 10.
// "0-0" is a valid key.
// "-1" is valid and identical to "0-1".
// "-" is valid and identical to "0-0".
func (s *Stream) parseStreamKey(key string) (uint64, uint64, error) {
	if key == "*" {
		// special case: auto-generate
		timestamp := uint64(time.Now().UnixMilli())
		var seq uint64
		if timestamp == s.LastKey.LeftNr {
			seq = s.LastKey.RightNr + 1
		}
		return timestamp, seq, nil
	}

	// On each iteration we "apply the base (10)" to the previous value, and add the new
	// - '0' to transform the numeric ascii value to its integer counterpart
	addDigitToTotal := func(total uint64, char rune) (newTotal uint64, err error) {
		const MaxUint64 uint64 = ^uint64(0)
		const MaxUint64base uint64 = MaxUint64 / 10

		if char < 48 || char > 57 {
			return 0, errors.New("invalid stream entry key")
		}

		if total > MaxUint64base {
			return 0, errors.New("integer overflow")
		}
		newBase := total * 10
		newTotal = newBase + uint64(char-'0')
		if newTotal < newBase {
			// Since char is a rune, which is an int32, any overflow caused by the
			// addition above will result in a result that is lower
			return newTotal, errors.New("integer overflow")
		}
		return newTotal, nil
	}

	var result1 uint64
	var result2 uint64
	var i int
	var char rune
	var err error
	for i, char = range key {
		if char == '-' {
			goto secondLoop
		}
		result1, err = addDigitToTotal(result1, char)
		if err != nil {
			return 0, 0, err
		}
	}
	// If we _naturally_ exit the loop, we're missing a hyphen
	return 0, 0, errors.New("invalid stream entry key: no hyphen")

secondLoop:
	for _, char := range key[i+1:] {
		// handle wildcard "*"
		if char == '*' {
			if result1 == s.LastKey.LeftNr {
				result2 = s.LastKey.RightNr + 1
			} else {
				result2 = 0
			}
			break
		}

		result2, err = addDigitToTotal(result2, char)
		if err != nil {
			return 0, 0, err
		}
	}

	return result1, result2, nil
}

// Represent `val` as a base64 number in `buf`. Each value in `buf` is one digit
// of the base64-represented number. The symbols used start at 1; all values will be
// between 1 and 64, inclusive.
func toBase64(buf []uint8, val uint64) {
	i := len(buf)
	for val >= 64 {
		i--
		buf[i] = uint8(val&31) + 1
		val >>= 6 // == number of trailing zero bits in 64
	}

	i--
	buf[i] = uint8(val)
}
