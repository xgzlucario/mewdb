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

	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("%08d", i)
		if err := db.Put(k, []byte(k)); err != nil {
			panic(err)
		}
	}
}
