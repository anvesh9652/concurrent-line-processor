package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
)

func IFNull[T any](org *T, def T) T {
	if org != nil {
		return *org
	}
	return def
}

func ExistOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stdout, "Error: %s\n", err)
		os.Exit(1)
	}
}

func WithNewLine(data []byte) []byte {
	return append(data, '\n')
}

func ErrWithDebugStack(err error) error {
	return errors.Join(err, fmt.Errorf("Debug Error Statck: %s\n", debug.Stack()))
}

func PrintAsJsonString(d any) {
	b, _ := json.MarshalIndent(d, "", "  ")
	fmt.Println(string(b))
}
