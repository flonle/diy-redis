package resp3

import (
	"strconv"
	"unsafe"
)

const (
	simpleStrPrefix = '+'
	simpleErrPrefix = '-'
	numberPrefix    = ':'
	bulkStrPrefix   = '$'
	arrPrefix       = '*'
	mapPrefix       = '%'
	setPrefix       = '~'
	nullType        = '_'
	CRLF            = "\r\n"
)

var nullSlice []byte = []byte("_\r\n")

// Big boy struct; the buffer is an exported field to mutate as you like. This exists mainly
// to attach a bunch of convenience methods that may aid in encoding some object into a
// respectable RESP3 counterpart.
type Encoder struct {
	Buf []byte
}

func (e *Encoder) Reset() { e.Buf = nil }

// Write a RESP null.
func (e *Encoder) WriteNull() {
	e.Buf = append(e.Buf, nullSlice...)
}

func (e *Encoder) WriteBulkStr(val string) {
	e.Buf = append(e.Buf, bulkStrPrefix)
	e.Buf = append(e.Buf, strconv.Itoa(len(val))...)
	e.Buf = append(e.Buf, CRLF...)
	e.Buf = append(e.Buf, val...)
	e.Buf = append(e.Buf, CRLF...)
}

// Don't forget to write the items, too.
func (e *Encoder) WriteArrHeader(arrLen int) {
	e.Buf = append(e.Buf, arrPrefix)
	e.Buf = append(e.Buf, strconv.Itoa(arrLen)...)
	e.Buf = append(e.Buf, CRLF...)
}

// This string shares a pointer with the internal buffer to avoid a copy. Therefore, a
// reset is mandatory to guarantee the immutability of the returned string.
func (e *Encoder) StringAndReset() (str string) {
	str = unsafe.String(unsafe.SliceData(e.Buf), len(e.Buf))
	e.Reset()
	return str
}

// Please don't use
// func VeryUnsafeStrToBytes(s string) []byte {
// 	p := unsafe.StringData(s)
// 	return unsafe.Slice(p, len(s))
// }
