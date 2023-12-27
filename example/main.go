package main

import (
	"fmt"

	"github.com/xgzlucario/mewdb"
)

func main() {
	db, err := mewdb.Open(&mewdb.Option{DirPath: "./data"})
	if err != nil {
		panic(err)
	}

	// put
	for i := 0; i < 10000; i++ {
		k := make([]byte, 1024*1024)
		k[i] = 255
		if err := db.Put(k, k); err != nil {
			panic(err)
		}
	}

	// get
	for i := 0; i < 10000; i++ {
		k := []byte(fmt.Sprintf("%08d", i))
		v, err := db.Get(k)
		if err != nil {
			panic(err)
		}
		if string(v) != string(k) {
			panic(fmt.Errorf("bug: invalid value: %s", v))
		}
	}

	db.Close()
}
