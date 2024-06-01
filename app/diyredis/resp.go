package diyredis

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unsafe"
)

// RESP array of bulk strings -> Go array of strings
func ParseCommand(reader *bufio.Reader) ([]string, error) {
	unit, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if unit[0] != '*' {
		return nil, fmt.Errorf("expected RESP array (*), got: %v", unit[0])
	}
	arrayLength, err := strconv.Atoi(unit[1 : len(unit)-2])
	if err != nil {
		return nil, err
	}

	command := make([]string, arrayLength)
	for i := range arrayLength {
		bulkStrHeader, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if bulkStrHeader[0] != '$' {
			return nil, fmt.Errorf("expected RESP bulk string ($), got: %v", bulkStrHeader[0])
		}
		bulkStrLen, err := strconv.Atoi(bulkStrHeader[1 : len(bulkStrHeader)-2])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, bulkStrLen+2) // +2 is for the \r\n at the end of the bulk string
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return nil, err
		}
		command[i] = string(buf[:len(buf)-2])
	}
	return command, nil

}

// Go string -> RESP bulk string
func MakeBulkStr(input string) []byte {
	inputLen := len(input)
	inputLenStr := strconv.Itoa(inputLen)
	res := make([]byte, inputLen+len(inputLenStr)+5)

	res[0] = '$'
	copy(res[1:], strToBytes(inputLenStr))
	i := len(inputLenStr) + 1
	res[i] = '\r'
	i++
	res[i] = '\n'
	i++
	copy(res[i:], strToBytes(input))
	i += inputLen
	res[i] = '\r'
	i++
	res[i] = '\n'
	return res
}

// Go slice -> RESP array
// Only supports strings rn sorry
func MakeArray(input []any) []byte {
	var sb strings.Builder
	sb.WriteString("*")
	sb.WriteString(strconv.Itoa(len(input)))
	sb.WriteString("\r\n")
	for _, v := range input {
		switch v := v.(type) {
		case string:
			sb.Write(MakeBulkStr(v))
		}
	}
	return strToBytes(sb.String())
}

func bytesToStr(b []byte) string {
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}

func strToBytes(s string) []byte {
	p := unsafe.StringData(s)
	return unsafe.Slice(p, len(s))
}
