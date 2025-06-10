package main

import "io"

type (
	Option        func(*ParallelReader)
	LineProcessor func([]byte) ([]byte, error)
)

type Chunk struct {
	id   int
	data []byte
}

type ParallelReader struct {
	r         io.Reader
	chunkSize int
	workers   int
	rowsRead  int64

	customLineProcessor LineProcessor

	inStream  chan *Chunk
	outStream chan *Chunk

	rowsReadLimit int

	pr *io.PipeReader
	pw *io.PipeWriter
}
