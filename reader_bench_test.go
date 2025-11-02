package concurrentlineprocessor

import (
	"fmt"
	"io"
	"os"
	"path"
	"testing"
)

var files = []string{
	Files[0],
	Files[1],
	Files[2],
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
		_, name := path.Split(f)
		b.Run(fmt.Sprintf("NormalReader-%s", name), func(b *testing.B) {
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
Old(master) code stats:
goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/concurrent-line-processor
cpu: Apple M1 Pro
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/data/temp_example.csv-10         	    7088	    166716 ns/op	  323478 B/op	      61 allocs/op
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/2024-06-04-details.jsonl-10  	     445	   3055034 ns/op	 1981411 B/op	     595 allocs/op
PASS
ok  	github.com/anvesh9652/concurrent-line-processor	2.713s
--------
=> new processSingleChunk function; no sync pool for line details
goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/concurrent-line-processor
cpu: Apple M1 Pro
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/data/temp_example.csv-10         	    6495	    175650 ns/op	  318295 B/op	      71 allocs/op
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/2024-06-04-details.jsonl-10  	     398	   3072475 ns/op	 2196996 B/op	     857 allocs/op
PASS
ok  	github.com/anvesh9652/concurrent-line-processor	2.861s

----------------------
=> added sync.Pool for LineDetails

Running tool: /Users/agali/installations/go/bin/go test -benchmem -run=^$ -bench ^BenchmarkParallelReader$ github.com/anvesh9652/concurrent-line-processor

goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/concurrent-line-processor
cpu: Apple M1 Pro
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/data/temp_example.csv-10         	    6060	    178646 ns/op	  313410 B/op	      72 allocs/op
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/2024-06-04-details.jsonl-10  	     427	   2890153 ns/op	 2191572 B/op	     617 allocs/op
BenchmarkParallelReader/ParallelReader_-_/Users/agali/Downloads/temp/my_data/usage_data_12m.json-10                                                     	       1	4269672375 ns/op	20585568 B/op	  525084 allocs/op
PASS
ok  	github.com/anvesh9652/concurrent-line-processor	8.107s

----------------------
-- Current Final Version Stats --
goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/concurrent-line-processor
cpu: Apple M1 Pro
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/data/temp_example.csv-10         	    7879	    148830 ns/op	  273712 B/op	      64 allocs/op
BenchmarkParallelReader/ParallelReader_-_/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/2024-06-04-details.jsonl-10  	     414	   3345926 ns/op	 2563337 B/op	     126 allocs/op
BenchmarkParallelReader/ParallelReader_-_/Users/agali/Downloads/temp/my_data/usage_data_12m.json-10                                                     	       1	3877387209 ns/op	 4337072 B/op	     295 allocs/op
PASS
ok  	github.com/anvesh9652/concurrent-line-processor	6.653s

12m rows: 99.9438185128% allocations reduced
*/

/*
benchstat <(GOMAXPROCS=5 go test -benchmem -run=^$ -bench="^(BenchmarkParallelReader|BenchmarkNormalReader)$" -count 6 .)
goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/concurrent-line-processor
cpu: Apple M1 Pro
                                                         │  /dev/fd/11  │
                                                         │    sec/op    │
NormalReader/NormalReader-temp_example.csv-5               96.85µ ±  6%
NormalReader/NormalReader-2024-06-04-details.jsonl-5       2.409m ± 24%
NormalReader/NormalReader-usage_data_12m.json-5             3.549 ±  9%
ParallelReader/ParallelReader-temp_example.csv-5           138.2µ ± 24%
ParallelReader/ParallelReader-2024-06-04-details.jsonl-5   3.132m ± 14%
ParallelReader/ParallelReader-usage_data_12m.json-5         4.090 ± 12%
geomean                                                    10.66m

                                                         │   /dev/fd/11   │
                                                         │      B/op      │
NormalReader/NormalReader-temp_example.csv-5                 224.0 ±   0%
NormalReader/NormalReader-2024-06-04-details.jsonl-5         240.0 ±   6%
NormalReader/NormalReader-usage_data_12m.json-5              888.0 ± 925%
ParallelReader/ParallelReader-temp_example.csv-5           249.6Ki ±   0%
ParallelReader/ParallelReader-2024-06-04-details.jsonl-5   2.071Mi ±  18%
ParallelReader/ParallelReader-usage_data_12m.json-5        13.49Mi ±  56%
geomean                                                    26.22Ki

                                                         │ /dev/fd/11  │
                                                         │  allocs/op  │
NormalReader/NormalReader-temp_example.csv-5               4.000 ±  0%
NormalReader/NormalReader-2024-06-04-details.jsonl-5       4.000 ±  0%
NormalReader/NormalReader-usage_data_12m.json-5            6.000 ± 33%
ParallelReader/ParallelReader-temp_example.csv-5           66.00 ±  0%
ParallelReader/ParallelReader-2024-06-04-details.jsonl-5   117.5 ±  8%
ParallelReader/ParallelReader-usage_data_12m.json-5        507.0 ± 44%
geomean                                                    26.88

*/

func BenchmarkParallelReader(b *testing.B) {
	for _, f := range files {
		_, name := path.Split(f)
		b.Run(fmt.Sprintf("ParallelReader-%s", name), func(b *testing.B) {
			for b.Loop() {
				r, err := getFileReader(f)
				FailOnErrorB(b, err)
				pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(b []byte, _ *LineDetails) ([]byte, error) {
					return b, nil
				}), WithWorkers(5))
				defer pr.Close()
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
		defer pr.Close()
		err = handleReadWrites(pr)
		fmt.Println(pr.RowsRead())
		FailOnErrorT(t, err)
	})
}

func NewTestParallelReader(r io.ReadCloser) *concurrentLineProcessor {
	custOp := func(b []byte, _ *LineDetails) ([]byte, error) {
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
