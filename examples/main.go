package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	clp "github.com/anvesh9652/concurrent-line-processor"
	"github.com/anvesh9652/concurrent-line-processor/profiling"
)

var (
	workers   = 8
	chunkSize = 1024 * 1024 * 4 // 4 MB

	mut = sync.Mutex{}
)

func main() {
	profiling.WithProfiling(start)
}

func start() {
	r, err := os.Open(clp.Files[0])
	clp.ExitOnError(err)
	defer r.Close()

	withTiming(func() {
		// GetAllKeys(r)
		// initConvertCtoJ(r)
		initConvertJtoC(clp.Files[2])
	})
}

func withTiming(f func()) {
	now := time.Now()
	f()
	fmt.Println("Total time took:", time.Since(now))
}
