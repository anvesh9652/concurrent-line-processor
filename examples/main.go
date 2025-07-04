package main

import (
	"os"
	"sync"

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
	r, err := os.Open(clp.Files[2])
	clp.ExitOnError(err)
	defer r.Close()

	GetAllKeys(r)
}
