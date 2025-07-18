package streams

import (
	"errors"
	"strconv"
	"time"
)

type Key struct {
	LeftNr  uint64
	RightNr uint64
}

type rxChar = uint8
type internalKey = []rxChar // internal representation of a stream entry key

var MaxKey = Key{MaxUint64, MaxUint64}
var MinKey = Key{0, 0}

func NewKey(key string, targetStream *Stream) (Key, error) {
	part1, part2, err := parseEntryKey(key, targetStream.LastEntry.Key)
	if err != nil {
		return Key{}, err
	}
	return Key{part1, part2}, nil
}

func (k Key) String() string {
	return strconv.FormatUint(k.LeftNr, 10) + "-" + strconv.FormatUint(k.RightNr, 10)
}

// Return the "next" higher key. e.g. "1-5" -> "1-6".
//
// Will overflow to Key{0,0}, but will let you know through 'overflow'.
func (k Key) Next() (key Key, overflow bool) {
	leftNr, rightNr := k.LeftNr, k.RightNr+1

	if rightNr == 0 { // overflow
		leftNr++

		if leftNr == 0 {
			// Overflow again! We're already at the highest possible key.
			overflow = true
		}
	}
	return Key{leftNr, rightNr}, overflow
}

// Return the "previous" lower key. e.g. "1-5" -> "1-4".
//
// Will underflow to Key{max, max}, but will let you know through 'underflow'.
func (k Key) Prev() (key Key, underflow bool) {
	leftNr, rightNr := k.LeftNr, k.RightNr-1

	if k.RightNr == MaxUint64 { // underflow
		leftNr--

		if leftNr == MaxUint64 {
			// Underflow again! We're already at the lowest possible key.
			underflow = true
		}
	}
	return Key{leftNr, rightNr}, underflow
}

// Return true if k is greater than k2
func (k Key) GreaterThan(k2 Key) bool {
	if k.LeftNr > k2.LeftNr {
		return true
	} else if k.LeftNr == k2.LeftNr && k.RightNr > k2.RightNr {
		return true
	}
	return false
}

// Return true if k is greater than k2
func (k Key) LesserThan(k2 Key) bool {
	if k.LeftNr < k2.LeftNr {
		return true
	} else if k.LeftNr == k2.LeftNr && k.RightNr < k2.RightNr {
		return true
	}
	return false
}

// Return true if k is equal to k2
func (k Key) EqualTo(k2 Key) bool {
	if k.LeftNr == k2.LeftNr && k.RightNr == k2.RightNr {
		return true
	}
	return false
}

// Return true if k is the lowest key possible
func (k Key) IsMin() bool {
	if k.LeftNr == 0 && k.RightNr == 0 {
		return true
	}
	return false
}

// Return true if k is the highest key possible
func (k Key) IsMax() bool {
	if k.LeftNr == MaxUint64 && k.RightNr == MaxUint64 {
		return true
	}
	return false
}

// Parse a stream entry key string, e.g. "123-123", into two integers, e.g. 123 & 123.
// Streamkeys always denote base 10.
//
//   - "-1" is valid and identical to "0-1", idem for "1-".
//   - "-" represents the lowest possible key, and "+" the highest.
//   - Accepts full wildcards (e.g. "*"), and partial wildcards (e.g. "123-*").
func parseEntryKey(key string, lastKeyUsed Key) (uint64, uint64, error) {
	if key == "-" {
		// special case: lowest key
		return 0, 0, nil
	}

	if key == "+" {
		// special case: highest key
		return MaxUint64, MaxUint64, nil
	}

	if key == "*" {
		// special case: auto-generate
		timestamp := uint64(time.Now().UnixMilli())
		var seq uint64
		if timestamp == lastKeyUsed.LeftNr {
			seq = lastKeyUsed.RightNr + 1
		}
		return timestamp, seq, nil
	}

	// On each iteration we "apply the base (10)" to the previous value, and add the new
	// - '0' to transform the numeric ascii value to its integer counterpart
	addDigitToTotal := func(total uint64, char rune) (newTotal uint64, err error) {
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
			if result1 == lastKeyUsed.LeftNr {
				result2 = lastKeyUsed.RightNr + 1
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

// Return the internal representation of `k`, for use in radix.go.
func (k Key) internalRepr() internalKey {
	buf := make([]uint8, 22)
	toBase64(buf[:11], k.LeftNr)
	toBase64(buf[11:], k.RightNr)
	return buf
}

// Represent `val` as a base64 number in `buf`. Each value in `buf` is one digit
// of the base64-represented number. All values will be between 0 and 63, inclusive.
func toBase64(buf []uint8, val uint64) {
	i := len(buf)
	for val >= 64 {
		i--
		buf[i] = uint8(val & 63)
		val >>= 6 // == number of trailing zero bits in 64
	}

	i--
	buf[i] = uint8(val)
}
