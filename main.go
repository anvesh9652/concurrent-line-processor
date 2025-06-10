package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

var (
	file1 = "/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/data/temp_example.csv"
	file2 = "/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/tmp/2024-06-04-detail.jsonl"
	file4 = "/Users/agali/Desktop/Work/go-lang/tryouts/1brc/src_data.txt"
)

func main() {
	now := time.Now()
	start()
	fmt.Println("took:", time.Since(now))
}

func start() {
	r, err := os.Open(file4)
	ExistOnError(err)
	customProcessor := func(b []byte) ([]byte, error) {
		return b, nil
	}

	nr := NewReader(r, WithChunkSize(1024*1024*10), WithWorkers(12), WithCustomLineProcessor(customProcessor))
	_, err = io.Copy(io.Discard, nr)
	ExistOnError(err)
	fmt.Println(nr.RowsRead())
}
