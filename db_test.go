package mewdb

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
)

var source = rand.NewSource(time.Now().UnixNano())

func randInt(max int64) int64 {
	n := source.Int63()
	for n > max {
		n = n >> 1
	}
	return n
}

func randKey() []byte {
	return []byte(fmt.Sprintf("%08x", source.Int63()))
}

func randKeydir() Keydir {
	return &wal.ChunkPosition{
		SegmentId:   uint32(randInt(math.MaxUint32)),
		BlockNumber: uint32(randInt(math.MaxUint32)),
		ChunkOffset: randInt(math.MaxUint32),
		ChunkSize:   uint32(randInt(math.MaxUint32)),
	}
}

func TestIndex(t *testing.T) {
	assert := assert.New(t)
	const N = 10000

	t.Run("index-set", func(t *testing.T) {
		index := NewIndex()
		validMap := map[string]Keydir{}

		// set
		for i := 0; i < N; i++ {
			key, keydir := randKey(), randKeydir()
			index.Set(key, keydir)
			validMap[string(key)] = keydir

			// check length.
			assert.Equal(index.Len(), len(validMap))
		}

		// get
		for k, v := range validMap {
			keydir, ok := index.Get([]byte(k))
			assert.True(ok)
			assert.Equal(keydir, v)
		}

		// scan
		count := 0
		index.Scan(func(key []byte, keydir Keydir) bool {
			count++
			v, ok := validMap[string(key)]
			assert.True(ok)
			assert.Equal(keydir, v)
			return false
		})
		assert.Equal(count, N)
	})
}

func TestRecord(t *testing.T) {
	assert := assert.New(t)

	t.Run("logRecord", func(t *testing.T) {
		var record *LogRecord

		for i := 0; i < 10000; i++ {
			now := uint32(time.Now().Unix())
			key, val := randKey(), randKey()

			record = &LogRecord{
				Timestamp: now,
				Key:       key,
				Value:     val,
			}
			// encode
			buf := record.encode()

			// decode
			record.decode(buf)

			assert.Equal(int64(now)*timeCarry, record.TTL())
			assert.Equal(key, record.Key)
			assert.Equal(val, record.Value)
		}
	})

	t.Run("hintRecord", func(t *testing.T) {
		var record *HintRecord

		for i := 0; i < 10000; i++ {
			now := uint32(time.Now().Unix())
			key, keydir := randKey(), randKeydir()

			record = &HintRecord{
				Timestamp: now,
				Key:       key,
				Keydir:    keydir,
			}
			// encode
			buf := record.encode()

			// decode
			record.decode(buf)

			assert.Equal(int64(now)*timeCarry, record.TTL())
			assert.Equal(key, record.Key)
			assert.Equal(keydir, record.Keydir)
		}
	})
}
