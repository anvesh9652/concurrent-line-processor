// Package concurrentlineprocessor provides a high-performance, concurrent line-by-line processor for large files or streams.
//
// See reader.go for full package documentation and usage examples.
package concurrentlineprocessor

import (
	"io"
	"sync"
)

type (
	// Option is a function type for configuring ConcurrentLineProcessor instances.
	// Options are passed to NewConcurrentLineProcessor to customize behavior.
	Option func(*ConcurrentLineProcessor)

	// LineProcessor is a function type for processing individual lines.
	// It receives a line as []byte and returns the processed line and any error.
	// Implementations must be thread-safe as they may be called concurrently.
	LineProcessor func([]byte) ([]byte, error)
)

// Chunk represents a piece of data to be processed, containing an ID for ordering
// and a pointer to the actual data buffer.
type Chunk struct {
	id   int
	data *[]byte
}

// Metrics contains performance and processing statistics for a ConcurrentLineProcessor.
type Metrics struct {
	// BytesRead is the total number of bytes read from the source reader.
	// When RowsReadLimit is set, it might read more bytes than the transformed bytes.
	BytesRead int64 `json:"bytes_read"`
	// TransformedBytes is the total number of bytes after processing each line.
	TransformedBytes int64 `json:"transformed_bytes"`
	// RowsRead is the total number of rows read from the source reader.
	RowsRead int64 `json:"rows_read"`
	// TimeTook is the total time taken to read and process the data.
	TimeTook string `json:"time_took"`
}

// ConcurrentLineProcessor provides high-performance, concurrent line-by-line processing
// of large files or streams. It implements io.Reader, allowing processed data to be
// read using standard Go I/O patterns.
type ConcurrentLineProcessor struct {
	// srcReader is the source reader from which data will be read.
	srcReader io.Reader

	// chunkSize is the size of each chunk to be read from the source reader.
	chunkSize int
	// channelSize is the size of the channels used for input and output streams.
	channelSize int
	// workers is the number of goroutines that will process the input data.
	// If you want sequential processing, set it to 1.
	workers int
	// rowsReadLimit is the limit on the number of rows to read. Default is -1, which means no limit.
	rowsReadLimit int

	// customLineProcessor allows you to process each line of the input data.
	// It is not thread-safe. You can't update anything outside of the function unless you use a mutex.
	customLineProcessor LineProcessor

	inStream  chan *Chunk
	outStream chan *Chunk

	pool sync.Pool

	pr *io.PipeReader
	pw *io.PipeWriter

	// metrics holds the metrics of the reading process, such as bytes read, transformed bytes, and rows read.
	metrics Metrics
}
