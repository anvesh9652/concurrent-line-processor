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
	"/Users/agali/Downloads/temp/my_data/usage_data_12m.json",
	"/Users/agali/Downloads/temp/my_data/usage_data_3m.json",
	"/Users/agali/Desktop/Work/go-lang/tryouts/1brc/gen/measurements.txt",
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

func AppendNewLine(chunk *Chunk) {
	if chunk == nil || chunk.endingPos == 0 {
		return
	}
	if chunk.data[chunk.endingPos-1] != '\n' {
		if chunk.endingPos < len(chunk.data) {
			chunk.data[chunk.endingPos] = '\n'
		} else {
			chunk.data = append(chunk.data, '\n')
		}
		chunk.endingPos++
	}
}

func ErrWithDebugStack(err error) error {
	return errors.Join(err, fmt.Errorf("Debug Error Stack: %s\n", debug.Stack()))
}

func PrintAsJsonString(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

func FormatBytes(size float64) string {
	formatValue := func(v float64, unit string) string {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", v), "0"), ".") + unit
	}

	if size < 1024 {
		return fmt.Sprintf("%.fB", size)
	} else if size < math.Pow(1024, 2) {
		return formatValue(size/1024, "KB")
	} else if size < math.Pow(1024, 3) {
		return formatValue(size/math.Pow(1024, 2), "MB")
	}
	return formatValue(size/math.Pow(1024, 3), "GB")
}

func FormatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return d.Round(time.Nanosecond).String()
	}
	if d < time.Millisecond {
		return d.Round(time.Microsecond).String()
	}
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	if d < time.Minute {
		// example: 1.234556sec. BTW same kind of logic applies for above cases
		// 1 * time.millisecond = +0.001sec rounding => btw(1.234 to 1.235) => after rounding 1.234s
		// 10 * time.Millisecond = +0.01sec rounding => btw(1.23 to 1.24) => after rounding 1.23s
		// 100 * time.Millisecond = +0.1sec rounding => btw(1.2 to 1.3) => after rounding 1.2s
		return d.Round(10 * time.Millisecond).String()
	}
	if d < time.Hour {
		return d.Round(time.Second).String()
	}
	return d.String()
}

func PrintSummaryPeriodically(p *concurrentLineProcessor) {
	t := time.NewTicker(5 * time.Second)
	for range t.C {
		fmt.Println(p.Summary())
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
