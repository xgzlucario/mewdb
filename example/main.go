package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/xgzlucario/mewdb"
)

var (
	// crop is 1K
	v = make([]byte, 1024)
)

func main() {
	start := time.Now()
	db, err := mewdb.Open(mewdb.DefaultOptions)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	fmt.Println("startup:", time.Since(start))

	for i := 0; i < (1000 * 10000); i++ {
		k := []byte(fmt.Sprintf("%08x", i))
		val, err := db.Get(k)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(val, v) {
			panic(fmt.Errorf("value not equal: %s %v %v", k, len(val), len(v)))
		}
	}

	// for i := 0; i < (1000 * 10000); i++ {
	// 	k := []byte(fmt.Sprintf("%08x", i))
	// 	if err := db.Put(k, v); err != nil {
	// 		panic(err)
	// 	}
	// }
}
