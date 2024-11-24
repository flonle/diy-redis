package diyredis

import (
	"errors"

	resp3 "github.com/codecrafters-io/redis-starter-go/app/diyredis/resp3"
	streams "github.com/codecrafters-io/redis-starter-go/app/diyredis/streams"
)

var EmptyRespArr []byte = []byte("*0\r\n")

// Encode a slice of entries into RESP. Only supports entries whose value is of type
// map[string]string.
//
// Will encode said map as a (RESP) array of key and values in order, just like in RESP2,
// even though RESP3 has support for maps.
func EntriesToRESP(entries []streams.Entry) ([]byte, error) {
	encoder := resp3.Encoder{}
	encoder.WriteArrHeader(len(entries))

	for _, entry := range entries {
		encoder.WriteArrHeader(2)
		encoder.WriteBulkStr(entry.Key.String())
		valMap, ok := entry.Val.(map[string]string)
		if !ok {
			return []byte{}, errors.New(
				"entry with wrong Val type; must be map[string]string",
			)
		}
		encoder.WriteArrHeader(len(valMap) * 2)
		for k, v := range valMap {
			encoder.WriteBulkStr(k)
			encoder.WriteBulkStr(v)
		}
	}

	return encoder.Buf, nil
}

func MakeRESParr(arr []string) []byte {
	encoder := resp3.Encoder{}
	encoder.WriteArrHeader(len(arr))
	for _, val := range arr {
		encoder.WriteBulkStr(val)
	}
	return encoder.Buf
}
