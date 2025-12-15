// Package concurrentlineprocessor provides a high-performance, concurrent line-by-line processor for large files or streams.
//
// See reader.go for full package documentation and usage examples, including configuration with multiple readers.
package concurrentlineprocessor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"math"
	"os"
	"runtime/debug"
	"strings"
	"time"
)

const (
	BaseSI     = 1000
	BaseBinary = 1024
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

// Lines returns an iterator over lines in the given byte slice.
// Each yielded line does not include the trailing newline character.
// Uses Go 1.23+ range-over-func iteration pattern.
func Lines(l []byte) iter.Seq[[]byte] {
	return func(yeild func([]byte) bool) {
		var s int
		for s < len(l) {
			i := bytes.IndexByte(l[s:], '\n')
			if i == -1 {
				i = len(l) - s
			}
			if !yeild(l[s : s+i : len(l)]) {
				return
			}
			s += i + 1
		}
	}
}

func ExitOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stdout, "Error: %s\n", err)
		os.Exit(1)
	}
}

// EnsureNewLineAtEnd ensures the chunk's data ends with a newline character.
// If the chunk is nil or empty, this is a no-op.
// It modifies the chunk in-place, either by setting an existing byte or appending.
func EnsureNewLineAtEnd(chunk *Chunk) {
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

// FormatBytes formats a byte count into a human-readable string.
// Use BaseSI (1000) for SI units (KB, MB, GB) or BaseBinary (1024) for binary units (KiB, MiB, GiB).
func FormatBytes(size, base float64) string {
	formatValue := func(v float64, unit string, base float64) string {
		if base == BaseBinary {
			unit = unit + "i"
		}
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", v), "0"), ".") + unit + "B"
	}

	if size < base {
		return fmt.Sprintf("%.fB", size)
	} else if size < math.Pow(base, 2) {
		return formatValue(size/base, "K", base)
	} else if size < math.Pow(base, 3) {
		return formatValue(size/math.Pow(base, 2), "M", base)
	}
	return formatValue(size/math.Pow(base, 3), "G", base)
}

// FormatDuration formats a duration into a human-readable string with appropriate precision.
// Precision decreases as duration increases: nanoseconds for tiny durations,
// milliseconds for sub-second, seconds for sub-minute, etc.
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

// PrintSummaryPeriodically prints the processor's summary at regular intervals.
// It stops when the context is cancelled. Useful for monitoring long-running processes.
func PrintSummaryPeriodically(ctx context.Context, p *concurrentLineProcessor, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			fmt.Println(p.Summary())
		case <-ctx.Done():
			return
		}
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

func Ptr[T any](v T) *T {
	return &v
}
