package main

import (
	"flag"
	"fmt"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rosedblabs/rosedb/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xgzlucario/mewdb"
)

var previousPause time.Duration

func gcPause() time.Duration {
	runtime.GC()
	var stats debug.GCStats
	debug.ReadGCStats(&stats)
	pause := stats.PauseTotal - previousPause
	previousPause = stats.PauseTotal
	return pause
}

func genKV(id int) ([]byte, []byte) {
	k := []byte(fmt.Sprintf("%08x", id))
	return k, k
}

/*
mewdb
entries: 20000000
alloc: 1180 mb
gcsys: 39 mb
heap inuse: 1180 mb
heap object: 4852 k
gc: 22
pause: 1.201491ms
cost: 59.878826592s

rosedb
entries: 20000000
alloc: 2297 mb
gcsys: 81 mb
heap inuse: 2423 mb
heap object: 70306 k
gc: 18
pause: 1.399241ms
cost: 1m14.041677021s

leveldb
entries: 20000000
alloc: 27 mb
gcsys: 5 mb
heap inuse: 27 mb
heap object: 175 k
gc: 83
pause: 4.261ms
cost: 1m7.401450028s

badger
entries: 20000000
alloc: 127 mb
gcsys: 6 mb
heap inuse: 128 mb
heap object: 599 k
gc: 59
pause: 9.784388ms
cost: 8.912119175s
*/

func main() {
	var c string
	var entries int

	flag.StringVar(&c, "db", "mewdb", "db to bench.")
	flag.IntVar(&entries, "entries", 2000*10000, "number of entries to test.")
	flag.Parse()

	fmt.Println("==========")
	fmt.Println(c)
	fmt.Println("entries:", entries)

	start := time.Now()
	switch c {
	case "mewdb":
		options := mewdb.DefaultOptions
		options.DirPath = "tmp-mewdb"
		db, _ := mewdb.Open(options)
		defer db.Close()
		for i := 0; i < entries; i++ {
			k, v := genKV(i)
			db.Put(k, v)
		}

	case "rosedb":
		options := rosedb.DefaultOptions
		options.DirPath = "tmp-rosedb"
		db, _ := rosedb.Open(options)
		defer db.Close()
		for i := 0; i < entries; i++ {
			k, v := genKV(i)
			db.Put(k, v)
		}

	case "leveldb":
		db, _ := leveldb.OpenFile("tmp-leveldb", nil)
		defer db.Close()
		for i := 0; i < entries; i++ {
			k, v := genKV(i)
			db.Put(k, v, nil)
		}

	case "badger":
		options := badger.DefaultOptions("tmp-badger")
		options.Logger = nil
		db, _ := badger.Open(options)
		defer db.Close()
		for i := 0; i < entries; i++ {
			k, v := genKV(i)
			db.View(func(txn *badger.Txn) error {
				return txn.Set(k, v)
			})
		}
	}
	cost := time.Since(start)

	var mem runtime.MemStats
	var stat debug.GCStats

	runtime.ReadMemStats(&mem)
	debug.ReadGCStats(&stat)

	fmt.Println("alloc:", mem.Alloc/1024/1024, "mb")
	fmt.Println("gcsys:", mem.GCSys/1024/1024, "mb")
	fmt.Println("heap inuse:", mem.HeapInuse/1024/1024, "mb")
	fmt.Println("heap object:", mem.HeapObjects/1024, "k")
	fmt.Println("gc:", stat.NumGC)
	fmt.Println("pause:", gcPause())
	fmt.Println("cost:", cost)
}
