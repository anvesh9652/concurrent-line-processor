package main

import "io"

type (
	Option        func(*ParallelReader)
	LineProcessor func([]byte) ([]byte, error)
)

type ParallelReader struct {
	r                   io.Reader
	chunkSize           int
	workers             int
	customLineProcessor LineProcessor

	rowsRead  int64
	inStream  chan []byte
	outStream chan []byte

	pr io.ReadCloser
	pw io.WriteCloser
}
