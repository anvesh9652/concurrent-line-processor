// Package concurrentlineprocessor provides a high-performance, concurrent line-by-line processor for large files or streams.
//
// See reader.go for full package documentation and usage examples, including configuration with multiple readers.
package concurrentlineprocessor

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"strings"
	"time"
)

// Files contains a list of test files used for development and testing.
// This variable is used internally for testing and benchmarking purposes.
var Files = []string{
	"/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/data/temp_example.csv",
	"/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/2024-06-04-details.jsonl",
	// "/Users/agali/Downloads/temp/my_data/usage_data_12m.json",
	// "/Users/agali/Desktop/Work/go-lang/tryouts/1brc/src_data.txt",
}

func IfNull[T any](org *T, def T) T {
	if org != nil {
		return *org
	}
	return def
}

func ExitOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stdout, "Error: %s\n", err)
		os.Exit(1)
	}
}

func AppendNewLine(b *[]byte) {
	if len(*b) > 0 && (*b)[len(*b)-1] != '\n' {
		*b = append(*b, '\n')
	}
}

func ErrWithDebugStack(err error) error {
	return errors.Join(err, fmt.Errorf("Debug Error Stack: %s\n", debug.Stack()))
}

func PrintAsJsonString(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

func FormatBytes(size int) string {
	formatValue := func(v float64, unit string) string {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", v), "0"), ".") + unit
	}

	sizef := float64(size)
	if sizef < 1024 {
		return fmt.Sprintf("%dB", size)
	} else if sizef < math.Pow(1024, 2) {
		return formatValue(sizef/1024, "KB")
	} else if sizef < math.Pow(1024, 3) {
		return formatValue(sizef/math.Pow(1024, 2), "MB")
	}
	return formatValue(sizef/math.Pow(1024, 3), "GB")
}

func PrintSummaryPeriodically(p *concurrentLineProcessor, now time.Time) {
	t := time.NewTicker(5 * time.Second)
	for range t.C {
		fmt.Printf("%s, time=%s\n", p.Summary(), time.Since(now))
	}
}

func Filter[T any](arr []T, keep func(T) bool) []T {
	var result []T
	for _, item := range arr {
		if keep(item) {
			result = append(result, item)
		}
	}
	return result
}
