package main

import (
	"fmt"

	"github.com/xgzlucario/mewdb"
)

func main() {
	db, err := mewdb.Open(mewdb.DefaultOptions)
	if err != nil {
		panic(err)
	}

	for i := 0; i < (1000 * 10000); i++ {
		k := []byte(fmt.Sprintf("%08x", i))
		v := make([]byte, 1024)
		if err := db.Put(k, v); err != nil {
			panic(err)
		}
	}

	db.Merge()
	select {}
}
