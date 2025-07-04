package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
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
	customProcessor := func(b []byte) ([]byte, error) {
		return b, nil
	}

	nr := NewReader(r, WithChunkSize(1024*1024*30), WithCustomLineProcessor(customProcessor))
	_, err = io.Copy(io.Discard, nr)
	ExistOnError(err)
	fmt.Println(nr.RowsRead())
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
