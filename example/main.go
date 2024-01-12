package main

import (
	"bytes"
	"fmt"

	"github.com/xgzlucario/mewdb"
)

func main() {
	db, err := mewdb.Open(mewdb.Options{DirPath: "data"})
	if err != nil {
		panic(err)
	}
	const N = 100 * 10000

	// put
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("%08d", i))
		if err := db.Put(k, k); err != nil {
			panic(err)
		}
	}

	// get
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("%08d", i))
		v, err := db.Get(k)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(k, v) {
			panic(fmt.Errorf("bug: invalid value: %s", v))
		}
	}

	db.Close()
}
