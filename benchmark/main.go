package main

import (
	"flag"
	"fmt"
	"runtime"
	"runtime/debug"
	"time"

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

func main() {
	var c string
	var entries int

	flag.StringVar(&c, "db", "mewdb", "db to bench.")
	flag.IntVar(&entries, "entries", 1000*10000, "number of entries to test.")
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

		// case "rosedb":
		// 	options := rosedb.DefaultOptions
		// 	options.DirPath = "tmp-rosedb"
		// 	db, _ := rosedb.Open(options)
		// 	defer db.Close()
		// 	for i := 0; i < entries; i++ {
		// 		k, v := genKV(i)
		// 		db.Put(k, v)
		// 	}

		// case "leveldb":
		// 	db, _ := leveldb.OpenFile("tmp-leveldb", nil)
		// 	defer db.Close()
		// 	for i := 0; i < entries; i++ {
		// 		k, v := genKV(i)
		// 		db.Put(k, v, nil)
		// 	}

		// case "flydb":
		// 	options := config.DefaultOptions
		// 	options.DirPath = "tmp-flydb"
		// 	db, _ := flydb.NewFlyDB(options)
		// 	defer db.Close()
		// 	for i := 0; i < entries; i++ {
		// 		k, v := genKV(i)
		// 		db.Put(k, v)
		// 	}
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
