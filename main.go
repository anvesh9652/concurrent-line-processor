package main

import (
	"fmt"
	"os"
)

func main() {
	r, err := os.Open("/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/data/temp_example.csv")
	ExistOnError(err)

	customProcessor := func(b []byte) ([]byte, error) {
		return b, nil
	}

	nr := NewReader(r, WithChunkSize(100), WithWorkers(1), WithCustomLineProcessor(customProcessor))
	err = nr.ReadDataWithChunkOperation(-1)
	ExistOnError(err)
	fmt.Println(nr.RowsRead())
}
