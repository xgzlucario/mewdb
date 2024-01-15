package main

import (
	"fmt"
	"os"
	"time"

	"github.com/xgzlucario/mewdb"
)

func main() {
	os.RemoveAll("data")

	db, err := mewdb.Open(mewdb.Options{DirPath: "data", MergeInterval: time.Second * 5})
	if err != nil {
		panic(err)
	}
	const N = 10000

	// put
	for i := 0; ; i++ {
		k := []byte(fmt.Sprintf("%08x", i%N))
		if err := db.Put(k, k); err != nil {
			panic(err)
		}
	}
}
