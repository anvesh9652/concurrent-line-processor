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
	// BytesRead is the total number of bytes read from the source reader.
	// When you have set RowsReadLimit, it might read more bytes than the transformed bytes.
	BytesRead int64 `json:"bytes_read"`
	// TransformedBytes is the total number of bytes after processing each line.
	TransformedBytes int64 `json:"transformed_bytes"`
	// RowsRead is the total number of rows read from the source reader.
	RowsRead int64 `json:"rows_read"`
	// TimeTook is the total time taken to read and process the data.
	TimeTook string `json:"time_took"`
}

type ParallelReader struct {
	// srcReader is the source reader from which data will be read.
	srcReader io.Reader

	// chunkSize is the size of each chunk to be read from the source reader.
	chunkSize int
	// workers is the number of goroutines that will process the input data.
	// If want sequential processing, set it to 1.
	workers int

	// customLineProcessor process allows you to process each line of the input data.
	// It is not thread-safe. you can't update something outside of the function unless you use a mutex.
	customLineProcessor LineProcessor

	inStream  chan *Chunk
	outStream chan *Chunk

	pool sync.Pool

	// rowsReadLimit is the limit on the number of rows to read. default is -1 which means no limit.
	rowsReadLimit int

	pr *io.PipeReader
	pw *io.PipeWriter

	// metrics holds the metrics of the reading process such as bytes read, transformed bytes, and rows read.
	metrics Metrics
}
