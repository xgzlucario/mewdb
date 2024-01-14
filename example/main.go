package main

import (
	"fmt"
	"os"

	"github.com/xgzlucario/mewdb"
)

func main() {
	os.RemoveAll("data")

	db, err := mewdb.Open(mewdb.Options{DirPath: "data"})
	if err != nil {
		panic(err)
	}
	const N = 30

	// put
	for i := 0; i < 100*10000; i++ {
		k := []byte(fmt.Sprintf("%08d", i%N))
		if err := db.Put(k, k); err != nil {
			panic(err)
		}
	}

	db.Merge()

	db.Close()
}
