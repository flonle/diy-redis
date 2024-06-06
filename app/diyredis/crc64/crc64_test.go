package crc64

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCRC64(t *testing.T) {
	hash := New()
	hash.Write([]byte("123456789"))
	sum := hash.Sum64()

	assert.Equal(t, uint64(16845390139448941002), sum)
}
