package mewdb

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	cache "github.com/xgzlucario/GigaCache"
)

func randKey() []byte {
	return []byte(fmt.Sprintf("%09x", cache.FastRand64()))
}

func randKeydir() Keydir {
	return &wal.ChunkPosition{
		SegmentId:   cache.FastRand(),
		BlockNumber: cache.FastRand(),
		ChunkOffset: int64(cache.FastRand64()),
		ChunkSize:   cache.FastRand(),
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
			index.Set(key, keydir, 0)
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
		index.Scan(func(key []byte, _ int64, keydir Keydir) bool {
			count++
			v, ok := validMap[string(key)]
			assert.True(ok)
			assert.Equal(keydir, v)
			return true
		})
		assert.Equal(count, N)
	})
}

func TestRecord(t *testing.T) {
	assert := assert.New(t)

	t.Run("varintSize", func(t *testing.T) {
		for i := 0; i < 10000; i++ {
			num := rand.IntN(math.MaxUint32)
			var bb []byte
			bb = binary.AppendUvarint(bb, uint64(num))
			assert.Equal(len(bb), varintSize(uint64(num)))
			assert.Equal(len(bb), varintSize(num))
		}
	})

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
			record.decodeAll(buf)

			assert.Equal(int64(now)*timeCarry, record.TTL())
			assert.Equal(key, record.Key)
			assert.Equal(val, record.Value)
		}
	})
}

func TestAPI(t *testing.T) {
	const N = 10 * 10000
	const testDir = "test-api"

	options := DefaultOptions
	options.MergeCronExpr = ""
	options.DirPath = testDir
	options.SegmentSize = MB

	t.Run("no-ttl", func(t *testing.T) {
		assert := assert.New(t)
		os.RemoveAll(testDir)

		db, err := Open(options)
		assert.Nil(err)
		vmap := make(map[string][]byte, N)

		// check function
		check := func() {
			for k, v := range vmap {
				val, err := db.Get([]byte(k))
				assert.Nil(err)
				assert.Equal(val, v)
			}
		}

		// set
		for i := 0; i < N; i++ {
			key, val := randKey(), randKey()
			db.Put(key, val)
			vmap[string(key)] = val
		}
		check()
		db.Close()

		// reopen
		db, err = Open(options)
		assert.Nil(err)
		check()

		// merge
		db.Merge()
		check()
	})
}
