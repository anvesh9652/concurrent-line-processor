package main

import (
	"encoding/json"
	"io"
	"os"
	"sync"

	"github.com/anvesh9652/concurrent-line-processor/profiling"
)

var (
	file1 = "/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/data/temp_example.csv"
	file2 = "/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/2024-06-04-detail.jsonl"
	file4 = "/Users/agali/Desktop/Work/go-lang/tryouts/1brc/src_data.txt"
)

var mut = sync.Mutex{}

func main() {
	profiling.WithProfiling(start)
}

func start() {
	r, err := os.Open(file4)
	ExistOnError(err)

	keys := map[string]bool{}
	customProcessor := func(b []byte) ([]byte, error) {
		return processByte(b, keys)
		// return b, nil
	}

	nr := NewReader(r,
		WithChunkSize(1024*1024*4), WithWorkers(8), WithRowsReadLimit(-1),
		WithCustomLineProcessor(customProcessor),
	)
	_, err = io.Copy(io.Discard, nr)
	ExistOnError(err)
	PrintAsJsonString(nr.Metrics())
}

func processByte(b []byte, keys map[string]bool) ([]byte, error) {
	var d map[string]any
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, err
	}

	d["test_key"] = "temp value"
	mut.Lock()
	for k := range d {
		keys[k] = true
	}
	mut.Unlock()
	// return json.Marshal(d)
	return b, nil
}
