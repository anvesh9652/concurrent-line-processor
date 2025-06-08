package main

import (
	"fmt"
	"io"
	"os"
	"testing"
)

var files = []string{
	file1,
	file2,

}

/*
goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/parallel-reader
cpu: Apple M1 Pro
BenchmarkNormalReader/NormalReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/data/temp_example.csv-10         	   15103	     77764 ns/op	     209 B/op	       4 allocs/op
BenchmarkNormalReader/NormalReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/tmp/2024-06-04-detail.jsonl-10   	     632	   2047697 ns/op	     223 B/op	       4 allocs/op
PASS
ok  	github.com/anvesh9652/parallel-reader	2.993s
*/

func BenchmarkNormalReader(b *testing.B) {
	for _, f := range files {
		b.Run(fmt.Sprintf("NormalReader - %s", f), func(b *testing.B) {
			for b.Loop() {
				r := getFileReader(b, f)
				FailOnError(b, handleReadWrites(r))
				r.Close()
			}
		})
	}
}


/*

goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/parallel-reader
cpu: Apple M1 Pro
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/data/temp_example.csv-10         	   10292	    115973 ns/op	  105042 B/op	      27 allocs/op
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/tmp/2024-06-04-detail.jsonl-10   	      81	  12901976 ns/op	18454994 B/op	    1087 allocs/op
PASS
ok  	github.com/anvesh9652/parallel-reader	2.662s

*/

func BenchmarkParallelReader(b *testing.B) {
	for _, f := range files {
		b.Run(fmt.Sprintf("ParallelReader - %s", f), func(b *testing.B) {
			for b.Loop() {
				r := getFileReader(b, f)
				pr := NewReader(r, WithCustomLineProcessor(func(b []byte) ([]byte, error) {
					return b, nil
				}), WithWorkers(5))
				FailOnError(b, handleReadWrites(pr))
			}
		})
	}
}

func FailOnError(b *testing.B, err error) {
	if err != nil {
		b.Error(err)
	}
}

func getFileReader(b *testing.B, file string) io.ReadCloser {
	f, err := os.Open(file)
	FailOnError(b, err)
	return f
}

func handleReadWrites(r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
}
