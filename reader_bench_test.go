package concurrentlineprocessor

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

var files = []string{
	Files[0],
	Files[1],
	Files[2],
	// Files[3],
}

/*
benchstat <(go test -benchmem -run=^$ -bench="^(BenchmarkParallelReader|BenchmarkNormalReader)$" -cpu=4 -count=6 -benchtime=5s .)
goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/concurrent-line-processor
cpu: Apple M1 Pro
                                                         │  /dev/fd/11  │
                                                         │    sec/op    │
NormalReader/NormalReader-temp_example.csv-4               95.27µ ±  4%
NormalReader/NormalReader-2024-06-04-details.jsonl-4       2.321m ± 10%
NormalReader/NormalReader-usage_data_12m.json-4             3.695 ± 11%
ParallelReader/ParallelReader-temp_example.csv-4           169.5µ ± 15%
ParallelReader/ParallelReader-2024-06-04-details.jsonl-4   3.151m ±  9%
ParallelReader/ParallelReader-usage_data_12m.json-4         3.923 ± 10%
geomean                                                    10.94m

                                                         │  /dev/fd/11   │
                                                         │     B/op      │
NormalReader/NormalReader-temp_example.csv-4                 288.0 ±  0%
NormalReader/NormalReader-2024-06-04-details.jsonl-4         289.0 ±  1%
NormalReader/NormalReader-usage_data_12m.json-4            4.559Ki ± 88%
ParallelReader/ParallelReader-temp_example.csv-4           253.3Ki ±  1%
ParallelReader/ParallelReader-2024-06-04-details.jsonl-4   1.813Mi ±  7%
ParallelReader/ParallelReader-usage_data_12m.json-4        18.14Mi ± 26%
geomean                                                    38.31Ki

                                                         │ /dev/fd/11  │
                                                         │  allocs/op  │
NormalReader/NormalReader-temp_example.csv-4               5.000 ± 20%
NormalReader/NormalReader-2024-06-04-details.jsonl-4       5.000 ± 20%
NormalReader/NormalReader-usage_data_12m.json-4            8.000 ± 12%
ParallelReader/ParallelReader-temp_example.csv-4           66.00 ±  0%
ParallelReader/ParallelReader-2024-06-04-details.jsonl-4   108.0 ±  4%
ParallelReader/ParallelReader-usage_data_12m.json-4        588.0 ± 23%
geomean                                                    30.71
*/

func BenchmarkNormalReader(b *testing.B) {
	for _, f := range files {
		_, name := path.Split(f)
		b.Run(fmt.Sprintf("NormalReader-%s", name), func(b *testing.B) {
			for b.Loop() {
				r, err := os.Open(f)
				require.NoError(b, err)
				defer r.Close()

				_, err = io.Copy(io.Discard, r)
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkParallelReader(b *testing.B) {
	for _, f := range files {
		_, name := path.Split(f)
		b.Run(fmt.Sprintf("ParallelReader-%s", name), func(b *testing.B) {
			for b.Loop() {
				r, err := os.Open(f)
				require.NoError(b, err)

				pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(b []byte, _ *LineDetails) ([]byte, error) {
					return b, nil
				}), WithWorkers(5))

				_, err = io.Copy(io.Discard, pr)
				require.NoError(b, err)

				err = pr.Close()
				require.NoError(b, err)
			}
		})
	}
}

/*
benchstat <(go test -benchmem -run=^$ -bench="^(BenchmarkUppercaseTransform)" -cpu=4 -count=6 -benchtime=5s .)
goos: darwin
goarch: arm64
pkg: github.com/anvesh9652/concurrent-line-processor
cpu: Apple M1 Pro
                                                                                              │  /dev/fd/11  │
                                                                                              │    sec/op    │
UppercaseTransform_NormalWay/NormalWay-temp_example.csv-4                                       105.5µ ±  6%
UppercaseTransform_NormalWay/NormalWay-2024-06-04-details.jsonl-4                               15.52m ±  3%
UppercaseTransform_NormalWay/NormalWay-usage_data_12m.json-4                                     19.30 ±  3%
UppercaseTransform_ConcurrentLineProcessor/ConcurrentLineProcessor-temp_example.csv-4           162.8µ ± 18%
UppercaseTransform_ConcurrentLineProcessor/ConcurrentLineProcessor-2024-06-04-details.jsonl-4   5.923m ±  3%
UppercaseTransform_ConcurrentLineProcessor/ConcurrentLineProcessor-usage_data_12m.json-4         5.150 ±  2%
geomean                                                                                         23.22m

                                                                                              │  /dev/fd/11  │
                                                                                              │     B/op     │
UppercaseTransform_NormalWay/NormalWay-temp_example.csv-4                                       4.376Ki ± 0%
UppercaseTransform_NormalWay/NormalWay-2024-06-04-details.jsonl-4                               16.59Ki ± 0%
UppercaseTransform_NormalWay/NormalWay-usage_data_12m.json-4                                    11.97Mi ± 0%
UppercaseTransform_ConcurrentLineProcessor/ConcurrentLineProcessor-temp_example.csv-4           258.1Ki ± 0%
UppercaseTransform_ConcurrentLineProcessor/ConcurrentLineProcessor-2024-06-04-details.jsonl-4   18.72Mi ± 1%
UppercaseTransform_ConcurrentLineProcessor/ConcurrentLineProcessor-usage_data_12m.json-4        22.95Mi ± 0%
geomean                                                                                         685.2Ki

                                                                                              │ /dev/fd/11  │
                                                                                              │  allocs/op  │
UppercaseTransform_NormalWay/NormalWay-temp_example.csv-4                                        46.00 ± 0%
UppercaseTransform_NormalWay/NormalWay-2024-06-04-details.jsonl-4                               12.56k ± 0%
UppercaseTransform_NormalWay/NormalWay-usage_data_12m.json-4                                    12.55M ± 0%
UppercaseTransform_ConcurrentLineProcessor/ConcurrentLineProcessor-temp_example.csv-4            66.00 ± 0%
UppercaseTransform_ConcurrentLineProcessor/ConcurrentLineProcessor-2024-06-04-details.jsonl-4    476.0 ± 1%
UppercaseTransform_ConcurrentLineProcessor/ConcurrentLineProcessor-usage_data_12m.json-4         653.0 ± 4%
geomean                                                                                         2.302k
*/

func BenchmarkUppercaseTransform_NormalWay(b *testing.B) {
	for _, f := range files {
		_, name := path.Split(f)
		b.Run(fmt.Sprintf("NormalWay-%s", name), func(b *testing.B) {
			for b.Loop() {
				r, err := os.Open(f)
				require.NoError(b, err)
				w := io.Discard

				scanner := bufio.NewScanner(r)
				for scanner.Scan() {
					line := scanner.Bytes()
					toUpperASCII(line)
					_, _ = w.Write(line)
					_, _ = w.Write([]byte{'\n'})
				}

				require.NoError(b, scanner.Err())
				require.NoError(b, r.Close())
			}
		})
	}
}

func BenchmarkUppercaseTransform_ConcurrentLineProcessor(b *testing.B) {
	for _, f := range files {
		_, name := path.Split(f)
		b.Run(fmt.Sprintf("ConcurrentLineProcessor-%s", name), func(b *testing.B) {
			for b.Loop() {
				r, err := os.Open(f)
				require.NoError(b, err)

				pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(line []byte, _ *LineDetails) ([]byte, error) {
					toUpperASCII(line)
					return line, nil
				}), WithWorkers(5))

				_, err = io.Copy(io.Discard, pr)
				require.NoError(b, err)

				err = pr.Close()
				require.NoError(b, err)
			}
		})
	}
}

// toUpperASCII uppercases ASCII letters in-place.
func toUpperASCII(b []byte) {
	for i := range b {
		if 'a' <= b[i] && b[i] <= 'z' {
			b[i] = b[i] - 32
		}
	}
}
