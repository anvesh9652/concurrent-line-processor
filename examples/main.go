package main

import (
	"fmt"
	"time"

	clp "github.com/anvesh9652/concurrent-line-processor"
	. "github.com/anvesh9652/concurrent-line-processor/examples/codes"
	"github.com/pkg/profile"
)

func main() {
	dir := "./profiling"
	// defer profile.Start(profile.CPUProfile, profile.ProfilePath(dir)).Stop()
	defer profile.Start(profile.MemProfile, profile.ProfilePath(dir), profile.MemProfileRate(1)).Stop()

	start()
}

func start() {
	// r, err := os.Open(clp.Files[3])
	// clp.ExitOnError(err)
	// defer r.Close()

	withTiming(func() {
		// GetAllKeys(r, -1)
		// InitConvertCtoJ(r)
		InitConvertJtoC(clp.Files[3])
		// MultiReaders(clp.Files)
	})
}

func withTiming(f func()) {
	now := time.Now()
	f()
	fmt.Println("Total time took:", clp.FormatDuration(time.Since(now)))
}
