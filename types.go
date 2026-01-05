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

	// DataProcessor is a function type for processing individual lines or chunks.
	// It receives the data as []byte, contextual info via ChunkDetails (containing ReaderID and ChunkID),
	// and an io.Writer to write the processed output to.
	//
	// The processor must write its output to the provided io.Writer rather than returning the result.
	// This design allows for efficient streaming without intermediate allocations.
	//
	// Implementations must be thread-safe as they may be called concurrently from multiple workers.
	// Do not mutate shared state without proper synchronization (e.g., sync.Mutex).
	//
	// Example:
	//
	//	func(b []byte, info *ChunkDetails, out io.Writer) error {
	//	    _, err := out.Write(bytes.ToUpper(b))
	//	    return err
	//	}
	DataProcessor func(b []byte, info *ChunkDetails, out io.Writer) error
)

// Chunk represents a piece of data to be processed.
// It implements io.Writer and io.ByteWriter for efficient data accumulation.
//
// Each chunk has an ID for ordering within a reader and a readerID to identify
// which source reader it came from (useful when processing multiple readers).
type Chunk struct {
	// data holds the raw bytes of the chunk.
	data []byte
	// id is the sequential chunk identifier within a single reader.
	id int
	// readerID identifies which source reader this chunk came from.
	readerID int

	// endingPos marks the end of valid data in the buffer.
	// Data beyond this position should be ignored as it may contain stale content
	// from previous pool reuse. This avoids repeated reslicing.
	endingPos int

	// rowsWritten tracks how many rows were written in this chunk after processing.
	rowsWritten int64
}

// ChunkDetails provides contextual information about the data being processed.
// It is passed to DataProcessor functions to provide context about the source.
// All fields use zero-based indexing.
type ChunkDetails struct {
	// ReaderID identifies which source reader this data came from.
	// Useful when processing multiple readers via WithReaders.
	ReaderID int
	// ChunkID is the sequential ID of the chunk within its source reader.
	// Can be used for ordering or debugging purposes.
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

// concurrentLineProcessor provides high-performance, concurrent processing
// of large files or streams. It implements io.Reader, allowing processed data to be
// read using standard Go I/O patterns like io.Copy, io.ReadAll, or bufio.Scanner.
//
// The processor supports two modes:
//   - Line processing: each line is processed individually (WithCustomLineProcessor)
//   - Chunk processing: entire chunks are processed at once (WithCustomChunkProcessor)
//
// Thread Safety:
//   - The Read method should be called from a single goroutine (standard io.Reader contract).
//   - Metrics can be read concurrently at any time.
//   - Custom processors are called concurrently and must be thread-safe.
type concurrentLineProcessor struct {
	chunkPool        sync.Pool
	chunkDetailsPool sync.Pool

	now time.Time

	// ctx is the context for managing cancellation and timeouts.
	ctx context.Context

	// customDataProcessor processes each line or chunk depending on isLineProcessor.
	// Must be thread-safe as it's called concurrently from multiple workers.
	customDataProcessor DataProcessor

	inStream  chan *Chunk
	outStream chan *Chunk

	pr *io.PipeReader
	pw *io.PipeWriter

	// readers holds multiple source readers for processing.
	readers []io.ReadCloser

	// metrics holds the metrics of the reading process, such as bytes read/written, rows read/written etc...
	metrics Metrics

	// chunkSize is the size of each chunk to be read from the source reader.
	chunkSize int
	// channelSize is the size of the channels used for input and output streams.
	channelSize int
	// workers is the number of goroutines that will process the input data.
	// If you want sequential processing, set it to 1.
	workers int
	// rowsReadLimit is the limit on the number of rows to read. Default is -1, which means no limit.
	rowsReadLimit int

	// isLineProcessor determines the processing mode:
	//   - nil: no custom processor, pass-through mode
	//   - true: line-by-line processing (WithCustomLineProcessor)
	//   - false: chunk processing (WithCustomChunkProcessor)
	isLineProcessor *bool
}

// Write implements io.Writer, appending src to the chunk's data buffer.
// It uses copy for efficiency when possible, falling back to append for overflow.
// The endingPos is updated to reflect the new data boundary.
func (chunk *Chunk) Write(src []byte) (int, error) {
	start := chunk.endingPos
	if copied := copy(chunk.data[start:], src); copied < len(src) {
		chunk.data = append(chunk.data, src[copied:]...) // append the remaining bytes
	}
	chunk.endingPos += len(src)
	return len(src), nil
}

func (chunk *Chunk) WriteByte(b byte) error {
	if chunk.endingPos < len(chunk.data) {
		chunk.data[chunk.endingPos] = b
		chunk.endingPos++
		return nil
	}
	chunk.data = append(chunk.data, b)
	chunk.endingPos++
	return nil
}

func (chunk *Chunk) Grow(n int) {
	available := min(cap(chunk.data)-chunk.endingPos, n)
	chunk.data = chunk.data[:chunk.endingPos+available]

	rem := n - available
	if rem > 0 {
		chunk.data = append(chunk.data, make([]byte, rem)...)
	}
}
