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

	for i := 0; i < 6976; i++ {
		k := []byte(fmt.Sprintf("%08d", i))
		if err := db.Put(k, k); err != nil {
			panic(err)
		}
	}
	db.Close()
}
