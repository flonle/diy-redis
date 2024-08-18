package diyredis

type Stream []StreamEntry

type StreamEntry struct {
	TimestampMs uint64
	SubID       uint64
	Vals        []string
}
