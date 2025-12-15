package concurrentlineprocessor

import (
	"bufio"
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
                                                         │ ./tmp/benchmark_outs/base1.txt │
                                                         │             sec/op             │
NormalReader/temp_example.csv-4                                  99.47µ ± 8%
NormalReader/2024-06-04-details.jsonl-4                          2.438m ± 4%
NormalReader/usage_data_12m.json-4                                3.540 ± 2%
ParallelReader/temp_example.csv-4                              160.3µ ± 3%
ParallelReader/2024-06-04-details.jsonl-4                      2.714m ± 8%
ParallelReader/usage_data_12m.json-4                            3.684 ± 1%
geomean                                                                       10.55m

                                                         │ ./tmp/benchmark_outs/base1.txt │
                                                         │              B/s               │
NormalReader/temp_example.csv-4                                 24.72Mi ± 9%
NormalReader/2024-06-04-details.jsonl-4                         6.213Gi ± 5%
NormalReader/usage_data_12m.json-4                              4.521Gi ± 2%
ParallelReader/temp_example.csv-4                             15.33Mi ± 3%
ParallelReader/2024-06-04-details.jsonl-4                     5.579Gi ± 7%
ParallelReader/usage_data_12m.json-4                          4.344Gi ± 1%
geomean                                                                      810.5Mi

                                                         │ ./tmp/benchmark_outs/base1.txt │
                                                         │              B/op              │
NormalReader/temp_example.csv-4                                 224.0 ±   0%
NormalReader/2024-06-04-details.jsonl-4                         225.5 ±   2%
NormalReader/usage_data_12m.json-4                              436.0 ± 942%
ParallelReader/temp_example.csv-4                           252.0Ki ±   0%
ParallelReader/2024-06-04-details.jsonl-4                   1.708Mi ±   6%
ParallelReader/usage_data_12m.json-4                        5.370Mi ± 112%
geomean                                                                    19.18Ki

                                                         │ ./tmp/benchmark_outs/base1.txt │
                                                         │           allocs/op            │
NormalReader/temp_example.csv-4                                  4.000 ±  0%
NormalReader/2024-06-04-details.jsonl-4                          4.000 ±  0%
NormalReader/usage_data_12m.json-4                               5.000 ± 20%
ParallelReader/temp_example.csv-4                              67.00 ±  0%
ParallelReader/2024-06-04-details.jsonl-4                      106.5 ±  3%
ParallelReader/usage_data_12m.json-4                           244.0 ± 71%
geomean                                                                       22.77

*/

func BenchmarkNormalReader(b *testing.B) {
	for _, f := range files {
		_, name := path.Split(f)
		b.Run(name, func(b *testing.B) {
			reportFileSize(b, f)
			for b.Loop() {
				r, err := os.Open(f)
				require.NoError(b, err)

				_, err = io.Copy(io.Discard, r)
				require.NoError(b, err)
				require.NoError(b, r.Close())
			}
		})
	}
}

func BenchmarkParallelReader(b *testing.B) {
	for _, f := range files {
		_, name := path.Split(f)
		b.Run(name, func(b *testing.B) {
			reportFileSize(b, f)
			for b.Loop() {
				r, err := os.Open(f)
				require.NoError(b, err)

				pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(line []byte, _ *ChunkDetails, w io.Writer) error {
					_, err := w.Write(line)
					return err
				}), WithWorkers(5))

				_, err = io.Copy(io.Discard, pr)
				require.NoError(b, err)
				require.NoError(b, pr.Close())
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
                                                                                              │ ./tmp/benchmark_outs/upper1.txt │
                                                                                              │             sec/op              │
UppercaseTransform_NormalWay/temp_example.csv-4                                                          97.69µ ±  3%
UppercaseTransform_NormalWay/2024-06-04-details.jsonl-4                                                  15.62m ±  5%
UppercaseTransform_NormalWay/usage_data_12m.json-4                                                        18.30 ±  2%
UppercaseTransform_ConcurrentLineProcessor/temp_example.csv-4                              155.9µ ± 10%
UppercaseTransform_ConcurrentLineProcessor/2024-06-04-details.jsonl-4                      5.736m ± 10%
UppercaseTransform_ConcurrentLineProcessor/usage_data_12m.json-4                            5.097 ±  3%
geomean                                                                                                            22.43m

                                                                                              │ ./tmp/benchmark_outs/upper1.txt │
                                                                                              │               B/s               │
UppercaseTransform_NormalWay/temp_example.csv-4                                                          25.15Mi ± 3%
UppercaseTransform_NormalWay/2024-06-04-details.jsonl-4                                                  993.0Mi ± 5%
UppercaseTransform_NormalWay/usage_data_12m.json-4                                                       895.4Mi ± 2%
UppercaseTransform_ConcurrentLineProcessor/temp_example.csv-4                              15.76Mi ± 9%
UppercaseTransform_ConcurrentLineProcessor/2024-06-04-details.jsonl-4                      2.640Gi ± 9%
UppercaseTransform_ConcurrentLineProcessor/usage_data_12m.json-4                           3.140Gi ± 3%
geomean                                                                                                            381.1Mi

                                                                                              │ ./tmp/benchmark_outs/upper1.txt │
                                                                                              │              B/op               │
UppercaseTransform_NormalWay/temp_example.csv-4                                                          4.376Ki ± 0%
UppercaseTransform_NormalWay/2024-06-04-details.jsonl-4                                                  16.59Ki ± 0%
UppercaseTransform_NormalWay/usage_data_12m.json-4                                                       11.97Mi ± 0%
UppercaseTransform_ConcurrentLineProcessor/temp_example.csv-4                              257.3Ki ± 0%
UppercaseTransform_ConcurrentLineProcessor/2024-06-04-details.jsonl-4                      18.75Mi ± 2%
UppercaseTransform_ConcurrentLineProcessor/usage_data_12m.json-4                           22.86Mi ± 1%
geomean                                                                                                            684.6Ki

                                                                                              │ ./tmp/benchmark_outs/upper1.txt │
                                                                                              │            allocs/op            │
UppercaseTransform_NormalWay/temp_example.csv-4                                                            46.00 ± 0%
UppercaseTransform_NormalWay/2024-06-04-details.jsonl-4                                                   12.56k ± 0%
UppercaseTransform_NormalWay/usage_data_12m.json-4                                                        12.55M ± 0%
UppercaseTransform_ConcurrentLineProcessor/temp_example.csv-4                                67.00 ± 0%
UppercaseTransform_ConcurrentLineProcessor/2024-06-04-details.jsonl-4                        478.0 ± 1%
UppercaseTransform_ConcurrentLineProcessor/usage_data_12m.json-4                             631.5 ± 3%
geomean                                                                                                             2.296k

*/

func BenchmarkUppercaseTransform_NormalWay(b *testing.B) {
	for _, f := range files {
		_, name := path.Split(f)
		b.Run(name, func(b *testing.B) {
			reportFileSize(b, f)
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
		b.Run(name, func(b *testing.B) {
			reportFileSize(b, f)
			for b.Loop() {
				r, err := os.Open(f)
				require.NoError(b, err)

				pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(line []byte, _ *ChunkDetails, w io.Writer) error {
					toUpperASCII(line)
					_, err := w.Write(line)
					return err
				}), WithWorkers(5))

				_, err = io.Copy(io.Discard, pr)
				require.NoError(b, err)
				require.NoError(b, pr.Close())
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

func reportFileSize(b *testing.B, f string) {
	file, err := os.Open(f)
	require.NoError(b, err)
	defer file.Close()

	info, err := file.Stat()
	require.NoError(b, err)
	size := info.Size()

	b.SetBytes(size) // <-- enables MB/s column
	b.ReportAllocs() // optional: show allocation stats (benchmem already does)
	b.ResetTimer()   // optional: make sure setup time is not counted
}
