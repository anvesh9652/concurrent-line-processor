package main

import (
	"io"
	"sync"
)

type (
	Option        func(*ParallelReader)
	LineProcessor func([]byte) ([]byte, error)
)

type Chunk struct {
	id   int
	data *[]byte
}

type Metrics struct {
	BytesRead        int
	TransformedBytes int
	RowsRead         int64
}

type ParallelReader struct {
	srcReader io.Reader

	chunkSize int
	workers   int

	customLineProcessor LineProcessor

	inStream  chan *Chunk
	outStream chan *Chunk

	pool sync.Pool

	rowsReadLimit int

	pr *io.PipeReader
	pw *io.PipeWriter

	Metrics Metrics
}
