// Package concurrentlineprocessor provides a high-performance, concurrent line-by-line processor for large files or streams.
//
// See reader.go for full package documentation and usage examples.
package concurrentlineprocessor

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
)

// Files contains a list of test files used for development and testing.
// This variable is used internally for testing and benchmarking purposes.
var Files = []string{
	"/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/data/temp_example.csv",
	"/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/2024-06-04-details.jsonl",
	"/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/transform-00002_1.csv.jsonl",
	"/Users/agali/Desktop/Work/go-lang/tryouts/1brc/src_data.txt",
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

func WithNewLine(data []byte) []byte {
	return append(data, '\n')
}

func ErrWithDebugStack(err error) error {
	return errors.Join(err, fmt.Errorf("Debug Error Stack: %s\n", debug.Stack()))
}

func PrintAsJsonString(d any) {
	b, _ := json.MarshalIndent(d, "", "  ")
	fmt.Println(string(b))
}
