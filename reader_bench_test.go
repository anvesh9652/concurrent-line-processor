package concurrentlineprocessor

import (
	"fmt"
	"io"
	"os"
	"testing"
)

var files = []string{
	Files[0],
	Files[1],
	// Files[2],
	// Files[3],
}

/*
goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/concurrent-line-processor
cpu: Apple M1 Pro
BenchmarkNormalReader/NormalReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/data/temp_example.csv-10         	    9636	    113762 ns/op	     209 B/op	       4 allocs/op
BenchmarkNormalReader/NormalReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/2024-06-04-details.jsonl-10   	     568	   2678503 ns/op	     239 B/op	       4 allocs/op
PASS
ok  	github.com/anvesh9652/concurrent-line-processor	2.935s
*/

func BenchmarkNormalReader(b *testing.B) {
	for _, f := range files {
		b.Run(fmt.Sprintf("NormalReader - %s", f), func(b *testing.B) {
			for b.Loop() {
				r, err := getFileReader(f)
				FailOnErrorB(b, err)
				FailOnErrorB(b, handleReadWrites(r))
				r.Close()
			}
		})
	}
}

/*
goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/concurrent-line-processor
cpu: Apple M1 Pro
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/data/temp_example.csv-10         	    7088	    166716 ns/op	  323478 B/op	      61 allocs/op
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/2024-06-04-details.jsonl-10  	     445	   3055034 ns/op	 1981411 B/op	     595 allocs/op
PASS
ok  	github.com/anvesh9652/concurrent-line-processor	2.713s
*/

func BenchmarkParallelReader(b *testing.B) {
	for _, f := range files {
		b.Run(fmt.Sprintf("ParallelReader - %s", f), func(b *testing.B) {
			for b.Loop() {
				r, err := getFileReader(f)
				FailOnErrorB(b, err)
				pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(b []byte) ([]byte, error) {
					return b, nil
				}), WithWorkers(5))
				FailOnErrorB(b, handleReadWrites(pr))
			}
		})
	}
}

func TestParallelReader(t *testing.T) {
	t.Run("Run Test", func(t *testing.T) {
		r, err := getFileReader(files[1])
		FailOnErrorT(t, err)
		pr := NewTestParallelReader(r)
		err = handleReadWrites(pr)
		fmt.Println(pr.RowsRead())
		FailOnErrorT(t, err)
	})
}

func NewTestParallelReader(r io.Reader) *ConcurrentLineProcessor {
	custOp := func(b []byte) ([]byte, error) {
		return b, nil
	}
	return NewConcurrentLineProcessor(r, WithCustomLineProcessor(custOp), WithWorkers(1))
}

// FailOnErrorB reports an error in a benchmark if err is not nil.
func FailOnErrorB(b *testing.B, err error) {
	if err != nil {
		b.Error(err)
	}
}

// FailOnErrorT reports an error in a test if err is not nil.
func FailOnErrorT(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}

func getFileReader(file string) (io.ReadCloser, error) {
	return os.Open(file)
}

func handleReadWrites(r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
}
