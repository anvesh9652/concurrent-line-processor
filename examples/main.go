package main

import (
	"fmt"
	"os"
	"time"

	clp "github.com/anvesh9652/concurrent-line-processor"
	. "github.com/anvesh9652/concurrent-line-processor/examples/codes"
	"github.com/anvesh9652/concurrent-line-processor/profiling"
)

func main() {
	profiling.WithProfiling(start)
}

func start() {
	r, err := os.Open(clp.Files[0])
	clp.ExitOnError(err)
	defer r.Close()

	withTiming(func() {
		// GetAllKeys(r, -1)
		// InitConvertCtoJ(r)
		InitConvertJtoC(clp.Files[2])
	})
}

func withTiming(f func()) {
	now := time.Now()
	f()
	fmt.Println("Total time took:", time.Since(now))
}
