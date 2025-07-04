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
		fmt.Printf("Error: %s, \nError Stack: %s\n", err.Error(), string(debug.Stack()))
		os.Exit(1)
	}
}

func WithNewLine(data []byte) []byte {
	return append(data, '\n')
}

func ErrWithDebugStack(err error) error {
	return errors.Join(err, fmt.Errorf("debug logs: %s", debug.Stack()))
}

func PrintAsJsonString(d any) {
	b, _ := json.MarshalIndent(d, "", "  ")
	fmt.Println(string(b))
}
