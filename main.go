package main

import (
	"fmt"
	"io"
	"os"
)

var (
	file1 = "/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/data/temp_example.csv"
	file2 = "/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/tmp/2024-06-04-detail.jsonl"
)

func main() {
	r, err := os.Open(file1)

	ExistOnError(err)

	customProcessor := func(b []byte) ([]byte, error) {
		return b, nil
	}

	nr := NewReader(r, WithChunkSize(100), WithWorkers(1), WithCustomLineProcessor(customProcessor))
	// err = nr.ReadDataWithChunkOperation(-1)
	data, err := io.ReadAll(nr)
	ExistOnError(err)
	printBytes(data)
	fmt.Println(nr.RowsRead())
	// nr.Close()
}
