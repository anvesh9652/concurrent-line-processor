package main

import (
	"fmt"
	"io"
	"os"
	"testing"
)

var files = []string{
	file1,
	// file2,
}

/*
goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/parallel-reader
cpu: Apple M1 Pro
BenchmarkNormalReader/NormalReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/parallel-reader/data/temp_example.csv-10         	   15008	     81404 ns/op	     208 B/op	       4 allocs/op
PASS
ok  	github.com/anvesh9652/parallel-reader	1.645s

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

func BenchmarkParallelReader(b *testing.B) {
	for _, f := range files {
		b.Run(fmt.Sprintf("NormalReader - %s", f), func(b *testing.B) {
			for b.Loop() {
				r := getFileReader(b, f)
				pr := NewReader(r, WithCustomLineProcessor(func(b []byte) ([]byte, error) {
					return b, nil
				}))
				FailOnError(b, handleReadWrites(pr))
				pr.Close()
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
