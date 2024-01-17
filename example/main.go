package main

import (
	"fmt"
	"time"

	"github.com/xgzlucario/mewdb"
)

func main() {
	start := time.Now()
	db, err := mewdb.Open(mewdb.Options{DirPath: "data", MergeInterval: time.Hour})
	if err != nil {
		panic(err)
	}
	fmt.Println("startup cost:", time.Since(start))

	// put
	// such as 1000*10000 1k picture.
	// for i := 0; i < 1000*10000; i++ {
	// 	if i/10000 == 0 {
	// 		fmt.Println("put", i, "w")
	// 	}
	// 	k := []byte(fmt.Sprintf("%08x", i))
	// 	v := make([]byte, 1024)

	// 	if err := db.Put(k, v); err != nil {
	// 		panic(err)
	// 	}
	// }
	db.Close()
}

// 1. put: append only
// 2. merge: [1, 2, 3, 4] SEG,
//     [1, 2] -> 1, [3, 4] -> 3]
// 1,2,3,4 -> 1,2,3,4
