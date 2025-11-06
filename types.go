// Package concurrentlineprocessor provides a high-performance, concurrent line-by-line processor for large files or streams.
//
// See reader.go for full package documentation and usage examples, including how to wire multiple io.ReadCloser sources into a single processor.
package concurrentlineprocessor

import (
	"context"
	"io"
	"sync"
	"time"
)

type (
	// Option is a function type for configuring concurrentLineProcessor instances.
	// Options are passed to NewConcurrentLineProcessor to customize behavior.
	Option func(*concurrentLineProcessor)

	// LineProcessor is a function type for processing individual lines.
	// It receives a line as []byte and info and then returns the processed line and any error.
	// Implementations must be thread-safe as they may be called concurrently.
	LineProcessor func(b []byte, info *LineDetails) ([]byte, error)
)

// Chunk represents a piece of data to be processed, containing an ID for ordering
// and a pointer to the actual data buffer.
type Chunk struct {
	id       int
	data     []byte
	readerID int

	// We don't want to do keep reslicing the data, use copy over append
	// So we keep track of where the data ends. data after this point is a junk.
	endingPos int

	// rowsWritten keeps track of how many rows were written in this chunk after processing.
	rowsWritten int64
}

// LineDetails provides contextual information about a line being processed.
// All the fields follow zero-based indexing.
type LineDetails struct {
	// ReaderID is the ID of the source reader from which this line was read.
	ReaderID int
	// ChunkID is the ID of the chunk which we have read from the source reader.
	ChunkID int
}

// Metrics contains performance and processing statistics for a concurrentLineProcessor.
type Metrics struct {
	// BytesRead is the total number of bytes read from the source reader.
	// When RowsReadLimit is set, it might read more bytes than the bytes written.
	BytesRead int64 `json:"bytes_read"`
	// BytesWritten is the total number of bytes written after processing each line.
	BytesWritten int64 `json:"bytes_written"`
	// RowsRead is the total number of rows read from the source reader.
	RowsRead int64 `json:"rows_read"`
	// RowsWritten is the total number of rows written to the output stream.
	RowsWritten int64 `json:"rows_written"`
	// TimeTook is the total time taken to read and process the data.
	TimeTook time.Duration `json:"time_took"`
}

// concurrentLineProcessor provides high-performance, concurrent line-by-line processing
// of large files or streams. It implements io.Reader, allowing processed data to be
// read using standard Go I/O patterns.
type concurrentLineProcessor struct {
	// ctx is the context for managing cancellation and timeouts.
	ctx context.Context

	// readers holds multiple source readers for processing.
	readers []io.ReadCloser

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

	// hasCustomLineProcessor indicates whether a custom line processor is set.
	// If true, the processor will use the customLineProcessor to process each line.
	hasCustomLineProcessor bool

	inStream  chan *Chunk
	outStream chan *Chunk

	chunkPool       sync.Pool
	lineDetailsPool sync.Pool

	pr *io.PipeReader
	pw *io.PipeWriter

	now time.Time

	// metrics holds the metrics of the reading process, such as bytes read/written, rows read/written etc...
	metrics Metrics
}
