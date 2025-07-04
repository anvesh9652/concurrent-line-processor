package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

var (
	file1 = "/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/data/temp_example.csv"
	file2 = "/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/tmp/2024-06-04-detail.jsonl"
	file4 = "/Users/agali/Desktop/Work/go-lang/tryouts/1brc/src_data.txt"
)

var (
	cporfile = flag.String("cprofile", "cpuprofile", " ")
	mporfile = flag.String("mprofile", "memprofile", " ")

	mut = sync.Mutex{}
)

func main() {
	cpuProfile()
	defer pprof.StopCPUProfile()
	now := time.Now()
	start()
	fmt.Println("took:", time.Since(now))
	memProfile()
}

func start() {
	r, err := os.Open(file4)
	ExistOnError(err)

	// keys := map[string]bool{}
	customProcessor := func(b []byte) ([]byte, error) {
		// return processByte(b, keys)
		return b, nil
	}

	nr := NewReader(r, WithChunkSize(1024*1024*4), WithWorkers(8), WithCustomLineProcessor(customProcessor))
	_, err = io.Copy(io.Discard, nr)
	ExistOnError(err)
	fmt.Println("Rows Read:", nr.RowsRead())
	PrintAsJsonString(nr.Metrics())
}

func cpuProfile() {
	if cporfile != nil {
		cf, _ := os.Create("./profiling/" + *cporfile + ".prof")
		if err := pprof.StartCPUProfile(cf); err != nil {
			log.Fatal(err)
		}
	}
}

func memProfile() {
	if mporfile != nil {
		mf, _ := os.Create("./profiling/" + *mporfile + ".prof")

		runtime.GC()
		if err := pprof.WriteHeapProfile(mf); err != nil {
			log.Fatal(err)
		}
		defer mf.Close()
	}
}

func processByte(b []byte, keys map[string]bool) ([]byte, error) {
	var d map[string]any
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, err
	}

	mut.Lock()
	for k := range d {
		keys[k] = true
	}
	mut.Unlock()
	d["test_key"] = "temp value"
	return json.Marshal(d)
}
