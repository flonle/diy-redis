// Uses a bitwise trie with bitmap, or "[Array Mapped Tree]" (AMT), but with a twist.
// Single-child nodes are also compressed, making this a Radix.
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
//
// [Array Mapped Tree]: https://infoscience.epfl.ch/server/api/core/bitstreams/607d2e29-f659-463b-b2e0-4b910300d2cf/content
package streams

import (
	"math/bits"
)

// A Radix tree node.
type RxNode struct {
	entry      *Entry // only leaves contain an entry
	bitmap     uint64
	extraChars []uint8 // extra characters (internal key symbols) for compressed single-child nodes. Any children of the node belongs to the last symbol in this field.
	children   []RxNode
}

// A key-value pair.
type Entry struct {
	Key Key
	Val any
}

// Find the node with the longest common prefix with `key`.
//
// Also returns the index, of `key`, where the search failed. If it never failed,
// this value will be -1 and `bestMatch` is guaranteed to be an *exact* match.
// Additionally, if the search failed while walking the node's `extraChars` field,
// the third return value will contain the failing index of `extraChars`.
// If it did not, this value will also be -1.
//
// > In a 'failed' state, where `failIdx != -1`, `bestMatch` has
// > no valid children (be it compressed or not) for `key[failIdx]`.
//
// > In a 'success' state, where `failIdx == -1`, `bestMatch` is
// > always a leaf node.
func (n *RxNode) longestCommonPrefix(key internalKey) (
	bestMatch *RxNode, failIdx int, extraFailIdx int,
) {
	var currentNode = n
	for depth := 0; ; depth++ {

		// If node is compressed, walk extraPrefix instead
		for i, char := range currentNode.extraChars {
			if char != key[depth+i] { // this cannot go out of bounds because keys are length 22, and so a node's extraChars length can never be more than (22 - node depth)
				// no match == end of search
				return currentNode, depth + i, i
			}
		}
		depth += len(currentNode.extraChars)

		if depth == len(key) {
			return currentNode, -1, -1 // we looped over all digits in key, either via `children` or via `extraPrefix`.
			// `i should always be == len(key) here, the first iteration value that is no longer a valid index into key.
			// Because the tree has a constant depth (not considering compression) of len(key),
			// we know we are at the deepest (leaf) node. `currentNode` will never have children here.
		}

		// child is not compressed and should thus be in `children`.
		bitmapOffset := key[depth]
		bitmask := uint64(1 << bitmapOffset)
		if currentNode.bitmap&bitmask == 0 { // no valid child
			return currentNode, depth, -1
		}
		currentNode = &currentNode.children[getChildIdx(currentNode.bitmap, bitmapOffset)]
	}
}

// Return a node satisfying `key`, starting from `n`, creating any nodes necessary.
func (n *RxNode) create(key internalKey) *RxNode {
	node, failIdx, extraFailIdx := n.longestCommonPrefix(key)
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
		splitNode.extraChars = node.extraChars[extraFailIdx+1:]
		// ↑ Note that I DO NOT create a new backing array; both `splitNode.extraChars`
		// and `node.extraChars` now share the same backing array.
		// This is not a problem because this is an append-only data structure and,
		// as a result, we never mutate the `extraChars` field. We only ever
		// create it, or split it.
		// We would not be able to do this if, for example, a Remove() method existed.
		// Then, we would have to append to an existing `extraChars` when re-compressing
		// after a deletion.

		// Fix current node by setting `extraChars` to only those before the split,
		// as well as setting its two new children: the split node and a new node
		// for the remaining digits in `key`
		splitNodeOffset := node.extraChars[extraFailIdx]
		newNodeOffset := key[failIdx]
		if newNodeOffset > splitNodeOffset {
			node.children = []RxNode{splitNode, {}}
			newNode = &node.children[1]
		} else {
			node.children = []RxNode{{}, splitNode}
			newNode = &node.children[0]
		}
		node.extraChars = node.extraChars[:extraFailIdx]
		node.bitmap = uint64(1 << splitNodeOffset)
		node.bitmap |= uint64(1 << newNodeOffset)
		node.entry = nil
	}

	// If there are any more symbols of `key` that need to be injected into the three,
	// we can compress them all into `newNode`, because we're inserting a single
	// value into the tree so no branches are possible from here to leaf
	lastPartOfKey := key[failIdx+1:]
	if len(lastPartOfKey) > 0 {
		newNode.extraChars = make([]uint8, len(lastPartOfKey))
		copy(newNode.extraChars, lastPartOfKey)
	}

	return newNode
}

// Make sure `childIdx` is a valid index in `children` of `n`. Will be an empty node.
func (n *RxNode) appendChild(childIdx int) {
	if n.children == nil {
		n.children = []RxNode{{}}
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

// Return entries under `n` with a key between `fromKey` and `toKey`, inclusively.
// Ordered from lowest to highest key.
func (n *RxNode) rangeEntries(fromKey internalKey, toKey internalKey) []Entry {
	var currentNode = n
	for depth := 0; ; depth++ {

		// Walk extraChars for compressed nodes.
		for i, char := range currentNode.extraChars {
			fromKeySymbol := fromKey[depth+i]
			toKeySymbol := toKey[depth+i]

			if fromKeySymbol == toKeySymbol && toKeySymbol == char {
				continue // all three symbols match
			}

			if fromKeySymbol == toKeySymbol {
				// fromKeySymbol and toKeySymbol match, but char does not.
				// Our resultset would be somewhere under fromKeySymbol/toKeySymbol, but since
				// no such child exists, no valid resultset exists.
				return []Entry{}
			}

			if fromKeySymbol < char && char < toKeySymbol {
				// char falls inside the range between fromKeySymbol and toKeySymbol;
				// all its children are valid. (All children are guaranteed to be between fromKey
				// and toKey.)
				return currentNode.getAllLeaves()
			}

			if char < fromKeySymbol || toKeySymbol < char {
				// char falls outside the range between fromKeySymbol and toKeySymbol;
				// none of its children are valid. (All children will either be too high or too
				// low.)
				return []Entry{}
			}

			if char == fromKeySymbol {
				// All entries in the current subtree are guaranteed to be lower than toKey.
				// Thus, all entries in the current subtree that are higher than fromKey is our
				// complete resultset.
				return currentNode.higherEntries(fromKey[depth:])
			}

			if char == toKeySymbol {
				// Same logic as above, but reversed.
				return currentNode.lowerEntries(toKey[depth:])
			}
		}

		depth += len(currentNode.extraChars)

		if depth == len(fromKey) {
			return []Entry{*currentNode.entry} // only happens when fromKey and toKey are identical
		}

		if fromKey[depth] == toKey[depth] {
			// fromKey an toKey (still) share a common path
			bitmapOffset := toKey[depth]
			bitmask := uint64(1 << bitmapOffset)
			if currentNode.bitmap&bitmask == 0 { // no valid child
				// Our resultset would be somewhere under the child for fromKey/toKey, but that
				// child does not exist. Therefore, no valid resultset exists.
				return []Entry{}
			} else {
				currentNode = &currentNode.children[getChildIdx(currentNode.bitmap, bitmapOffset)]
				continue
			}
		}

		// The path shared by fromKey and toKey deviate at the current node.
		result := []Entry{}
		fromKeyBitmask := uint64(1 << fromKey[depth])
		if currentNode.bitmap&fromKeyBitmask != 0 { // child exists
			fromNode := currentNode.children[getChildIdx(currentNode.bitmap, fromKey[depth])]
			result = append(result, fromNode.higherEntries(fromKey[depth+1:])...)
		}

		for i := fromKey[depth] + 1; i < toKey[depth]; i++ {
			bitmask := uint64(1 << i)
			if currentNode.bitmap&bitmask != 0 { // child exists
				childNode := currentNode.children[getChildIdx(currentNode.bitmap, i)]
				result = append(result, childNode.getAllLeaves()...)
			}
		}

		toKeyBitmask := uint64(1 << toKey[depth])
		if currentNode.bitmap&toKeyBitmask != 0 { // child exists
			toNode := currentNode.children[getChildIdx(currentNode.bitmap, toKey[depth])]
			result = append(result, toNode.lowerEntries(toKey[depth+1:])...)
		}

		return result
	}
}

// Return entries under `n` with a key higher than or equal to `key`, ordered from
// lowest to highest key.
func (n *RxNode) higherEntries(key internalKey) []Entry {
	higherNodes := n.higherSiblingsDFS(key)
	entries := make([]Entry, 0, len(higherNodes)) // AT LEAST as many leaves as there are nodes
	for i := len(higherNodes) - 1; i >= 0; i-- {
		// Reverse iteration because higherSiblingDFS returns from highest to lowest
		entries = append(entries, higherNodes[i].getAllLeaves()...)
	}
	return entries
}

// Return entries under `n` with a key lower than or equal to `key`, ordered from
// lowest to highest key.
func (n *RxNode) lowerEntries(key internalKey) []Entry {
	lowerNodes := n.lowerSiblingsDFS(key)
	entries := make([]Entry, 0, len(lowerNodes)) // AT LEAST as many leaves as there are nodes
	for _, node := range lowerNodes {
		entries = append(entries, node.getAllLeaves()...)
	}
	return entries
}

// Get `RxLeafInfo` of all leaves that are a child of `n`.
// Returns are ordered by key, lowest to highest.
func (n *RxNode) getAllLeaves() []Entry {
	entries := make([]Entry, 0, 1)

	nodeStack := []*RxNode{n}
	var node *RxNode
	// DFS w/ stack
	for len(nodeStack) > 0 {
		nodeStack, node = pop(nodeStack)
		if node.entry != nil {
			entries = append(entries, *node.entry)
		} else {
			nodeStack = appendPtrsReverse(nodeStack, node.children)
		}
	}

	return entries
}

// Return a set of nodes whose children all have a key that is higher or equal to `key`.
// They are ordered by key; highest to lowest.
//
// Note that this does not return *all* higher nodes -- it just does a DFS for `key`,
// grabbing any sibling nodes with a higher key at every level.
func (n *RxNode) higherSiblingsDFS(key internalKey) []*RxNode {
	result := []*RxNode{}
	var currentNode = n
	for depth := 0; ; depth++ {

		// If node is compressed, walk extraPrefix instead
		for ii, char := range currentNode.extraChars {
			if char < key[depth+ii] { // this cannot go out of bounds because keys are length 22, and so a node's extraChars length can never be more than (22 - node depth)
				// No keys under this node can ever be higher
				return result
			} else if char > key[depth+ii] {
				// All keys under this node are guaranteed to be higher
				return append(result, currentNode)
			}
			// If prefix == key[i+ii], we have a match and must continue
		}
		depth += len(currentNode.extraChars)

		if depth == len(key) {
			return append(result, currentNode) // just 'return rightSideNodes' for a non-inclusive result
		}

		// child is not compressed and should thus be in `children`.
		bitmapOffset := key[depth]
		bitmask := uint64(1 << bitmapOffset)
		childIdx := getChildIdx(currentNode.bitmap, bitmapOffset)

		if currentNode.bitmap&bitmask == 0 {
			// child does not exist: take all children higher than the hypothetical child, and return
			return appendPtrsReverse(result, currentNode.children[childIdx:])
		}

		// child exists: take all higher children and continue
		result = appendPtrsReverse(result, currentNode.children[childIdx+1:])
		// Note: children slices are always ordered from lowest to highest
		currentNode = &currentNode.children[childIdx]
	}
}

// Return a set of nodes whose children all have a key that is lower or equal to `key`.
// They are ordered by key; lowest to highest.
//
// Note that this does not return *all* higher nodes -- it just does a DFS for `key`,
// grabbing any sibling nodes with a higher key at every level.
func (n *RxNode) lowerSiblingsDFS(key internalKey) []*RxNode {
	result := []*RxNode{}
	var currentNode = n
	for depth := 0; ; depth++ {

		// if node is compressed, walk extraChars instead
		for ii, char := range currentNode.extraChars {
			if char > key[depth+ii] { // this cannot go out of bounds because keys are length 22, and so a node's extraChars length can never be more than (22 - node depth)
				// No keys under this node can ever be lower
				return result
			} else if char < key[depth+ii] {
				// All keys under this node are guaranteed to be lower
				return append(result, currentNode)
			}
			// If prefix == key[i+ii], we have a match and must continue
		}
		depth += len(currentNode.extraChars)

		if depth == len(key) {
			return append(result, currentNode) // just 'return leftSideNodes' for a non-inclusive result
		}

		// child is not compressed and should thus be in `children`.
		bitmapOffset := key[depth]
		bitmask := uint64(1 << bitmapOffset)
		childIdx := getChildIdx(currentNode.bitmap, bitmapOffset)

		if currentNode.bitmap&bitmask == 0 {
			// child does not exist: take all children lower than the hypothetical child, and return
			return appendPtrs(result, currentNode.children[:childIdx-1])
		}

		// child exists: take all lower children and continue
		result = appendPtrs(result, currentNode.children[:childIdx]) // todo: should this not also be appendPtrsReverse?
		// Note: children slices are always ordered from lowest to highest
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

func pop(s []*RxNode) ([]*RxNode, *RxNode) {
	val := s[len(s)-1]
	return s[:len(s)-1], val
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
