package diyredis

import (
	"bufio"
	"io"
	"os"
	"testing"
)

func BenchmarkReadEntireFile(b *testing.B) {
	for range b.N {
		f, _ := os.ReadFile("/home/flo/dev/build-your-own-x/diy-redis/dump.rdb")
		buf := make([]byte, 10)
		i := 0
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
		i += 10
		copy(f[i:], buf)
	}
}

func BenchmarkReadPartOfFile(b *testing.B) {
	for range b.N {
		f, _ := os.Open("/home/flo/dev/build-your-own-x/diy-redis/dump.rdb")
		buf := make([]byte, 10)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		_, _ = f.Read(buf)
		f.Close()
	}
}

func BenchmarkBufioReader(b *testing.B) {
	for range b.N {
		f, _ := os.Open("/home/flo/dev/build-your-own-x/diy-redis/dump.rdb")
		r := bufio.NewReader(f)
		buf := make([]byte, 10)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		io.ReadFull(r, buf)
		f.Close()
	}
}
