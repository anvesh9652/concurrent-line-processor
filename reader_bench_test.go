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
                                          │ ./tmp/benchmark_outs/base2.txt │
                                          │             sec/op             │
NormalReader/temp_example.csv-4                                101.2µ ± 2%
NormalReader/2024-06-04-details.jsonl-4                        2.281m ± 4%
NormalReader/usage_data_12m.json-4                              3.555 ± 7%
ParallelReader/temp_example.csv-4                              175.9µ ± 2%
ParallelReader/2024-06-04-details.jsonl-4                      2.910m ± 7%
ParallelReader/usage_data_12m.json-4                            3.829 ± 6%
geomean                                                        10.82m

                                          │ ./tmp/benchmark_outs/base2.txt │
                                          │              B/s               │
NormalReader/temp_example.csv-4                               24.28Mi ± 2%
NormalReader/2024-06-04-details.jsonl-4                       6.639Gi ± 3%
NormalReader/usage_data_12m.json-4                            4.501Gi ± 7%
ParallelReader/temp_example.csv-4                             13.97Mi ± 2%
ParallelReader/2024-06-04-details.jsonl-4                     5.204Gi ± 6%
ParallelReader/usage_data_12m.json-4                          4.180Gi ± 6%
geomean                                                       789.7Mi

                                          │ ./tmp/benchmark_outs/base2.txt │
                                          │              B/op              │
NormalReader/temp_example.csv-4                                224.0 ±  0%
NormalReader/2024-06-04-details.jsonl-4                        224.0 ±  1%
NormalReader/usage_data_12m.json-4                           4.438Ki ± 90%
ParallelReader/temp_example.csv-4                            255.9Ki ±  0%
ParallelReader/2024-06-04-details.jsonl-4                    841.3Ki ±  4%
ParallelReader/usage_data_12m.json-4                         8.169Mi ± 20%
geomean                                                      26.94Ki

                                          │ ./tmp/benchmark_outs/base2.txt │
                                          │           allocs/op            │
NormalReader/temp_example.csv-4                                4.000 ±  0%
NormalReader/2024-06-04-details.jsonl-4                        4.000 ±  0%
NormalReader/usage_data_12m.json-4                             6.000 ± 17%
ParallelReader/temp_example.csv-4                              67.00 ±  0%
ParallelReader/2024-06-04-details.jsonl-4                      92.00 ±  1%
ParallelReader/usage_data_12m.json-4                           431.0 ± 18%
geomean                                                        25.18


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
                                                                      │ ./tmp/benchmark_outs/upper2.txt │
                                                                      │             sec/op              │
UppercaseTransform_NormalWay/temp_example.csv-4                                             110.7µ ± 4%
UppercaseTransform_NormalWay/2024-06-04-details.jsonl-4                                     16.28m ± 2%
UppercaseTransform_NormalWay/usage_data_12m.json-4                                           19.25 ± 3%
UppercaseTransform_ConcurrentLineProcessor/temp_example.csv-4                               167.9µ ± 4%
UppercaseTransform_ConcurrentLineProcessor/2024-06-04-details.jsonl-4                       4.995m ± 3%
UppercaseTransform_ConcurrentLineProcessor/usage_data_12m.json-4                             5.082 ± 8%
geomean                                                                                     23.00m

                                                                      │ ./tmp/benchmark_outs/upper2.txt │
                                                                      │               B/s               │
UppercaseTransform_NormalWay/temp_example.csv-4                                            22.20Mi ± 5%
UppercaseTransform_NormalWay/2024-06-04-details.jsonl-4                                    952.3Mi ± 2%
UppercaseTransform_NormalWay/usage_data_12m.json-4                                         851.4Mi ± 3%
UppercaseTransform_ConcurrentLineProcessor/temp_example.csv-4                              14.63Mi ± 4%
UppercaseTransform_ConcurrentLineProcessor/2024-06-04-details.jsonl-4                      3.032Gi ± 3%
UppercaseTransform_ConcurrentLineProcessor/usage_data_12m.json-4                           3.149Gi ± 7%
geomean                                                                                    371.7Mi

                                                                      │ ./tmp/benchmark_outs/upper2.txt │
                                                                      │              B/op               │
UppercaseTransform_NormalWay/temp_example.csv-4                                            4.376Ki ± 0%
UppercaseTransform_NormalWay/2024-06-04-details.jsonl-4                                    16.59Ki ± 0%
UppercaseTransform_NormalWay/usage_data_12m.json-4                                         11.97Mi ± 0%
UppercaseTransform_ConcurrentLineProcessor/temp_example.csv-4                              261.4Ki ± 0%
UppercaseTransform_ConcurrentLineProcessor/2024-06-04-details.jsonl-4                      9.038Mi ± 1%
UppercaseTransform_ConcurrentLineProcessor/usage_data_12m.json-4                           9.711Mi ± 1%
geomean                                                                                    527.0Ki

                                                                      │ ./tmp/benchmark_outs/upper2.txt │
                                                                      │            allocs/op            │
UppercaseTransform_NormalWay/temp_example.csv-4                                              46.00 ± 0%
UppercaseTransform_NormalWay/2024-06-04-details.jsonl-4                                     12.56k ± 0%
UppercaseTransform_NormalWay/usage_data_12m.json-4                                          12.55M ± 0%
UppercaseTransform_ConcurrentLineProcessor/temp_example.csv-4                                67.00 ± 0%
UppercaseTransform_ConcurrentLineProcessor/2024-06-04-details.jsonl-4                        373.5 ± 1%
UppercaseTransform_ConcurrentLineProcessor/usage_data_12m.json-4                             470.0 ± 6%
geomean                                                                                     2.098k


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