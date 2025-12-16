// Package concurrentlineprocessor provides a high-performance, concurrent line-by-line processor for large files or streams.
//
// See reader.go for full package documentation and usage examples.
package concurrentlineprocessor

import (
	"context"
	"io"
)

// WithOpts applies the given options to the concurrentLineProcessor.
// This is a convenience function for applying multiple options at once.
func WithOpts(p *concurrentLineProcessor, opts ...Option) *concurrentLineProcessor {
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// WithChunkSize sets the chunk size for reading data from the source.
// Larger chunk sizes can improve performance for large files but may use more memory.
// The default chunk size is 64KB.
//
// Example:
//
//	clp.NewConcurrentLineProcessor(reader, clp.WithChunkSize(1024*1024)) // 1MB chunks
func WithChunkSize(size int) Option {
	return func(pr *concurrentLineProcessor) {
		pr.chunkSize = size
	}
}

// WithWorkers sets the number of worker goroutines for concurrent processing.
// More workers can improve performance for CPU-intensive line processing,
// but may not help for I/O-bound operations. The default is runtime.NumCPU().
//
// Example:
//
//	clp.NewConcurrentLineProcessor(reader, clp.WithWorkers(8))
func WithWorkers(n int) Option {
	return func(pr *concurrentLineProcessor) {
		pr.workers = n
	}
}

// WithCustomLineProcessor sets a custom function to process each line individually.
// The function receives a line as []byte (without trailing newline), a ChunkDetails
// struct with contextual info (ReaderID, ChunkID), and an io.Writer to write output to.
//
// The processor must write its output to the provided io.Writer. A newline is
// automatically appended after each processed line.
//
// The function must be thread-safe and should not modify external state
// without proper synchronization (e.g., sync.Mutex).
//
// Example:
//
//	// Convert lines to uppercase
//	processor := func(line []byte, info *clp.ChunkDetails, out io.Writer) error {
//	    _, err := out.Write(bytes.ToUpper(line))
//	    return err
//	}
//	clp.NewConcurrentLineProcessor(reader, clp.WithCustomLineProcessor(processor))
func WithCustomLineProcessor(c DataProcessor) Option {
	return func(pr *concurrentLineProcessor) {
		if c == nil {
			return
		}
		pr.isLineProcessor = Ptr(true)
		pr.customDataProcessor = c
	}
}

// WithCustomChunkProcessor sets a custom function to process entire chunks at once.
// Unlike WithCustomLineProcessor, this processes the full chunk buffer rather than
// individual lines, which can be more efficient for certain operations like aggregation.
//
// The function receives a chunk as []byte, a ChunkDetails struct with contextual info
// (ReaderID, ChunkID), and an io.Writer to write output to.
//
// The processor must write its output to the provided io.Writer. A newline is
// automatically ensured at the end of the chunk output.
//
// The function must be thread-safe and should not modify external state
// without proper synchronization (e.g., sync.Mutex).
//
// Example:
//
//	// Process entire chunk and extract all JSON keys
//	processor := func(chunk []byte, info *clp.ChunkDetails, out io.Writer) error {
//	    // Process chunk as a whole
//	    _, err := out.Write(chunk)
//	    return err
//	}
//	clp.NewConcurrentLineProcessor(reader, clp.WithCustomChunkProcessor(processor))
func WithCustomChunkProcessor(c DataProcessor) Option {
	return func(pr *concurrentLineProcessor) {
		if c == nil {
			return
		}
		pr.isLineProcessor = Ptr(false)
		pr.customDataProcessor = c
	}
}

// WithRowsReadLimit sets a limit on the number of rows to read from the source.
// Use -1 for no limit (default). This is useful for processing only a subset
// of a large file for testing or sampling purposes.
//
// Example:
//
//	clp.NewConcurrentLineProcessor(reader, clp.WithRowsReadLimit(1000)) // Process only first 1000 lines
func WithRowsReadLimit(limit int) Option {
	return func(pr *concurrentLineProcessor) {
		pr.rowsReadLimit = limit
	}
}

// WithChannelSize sets the size of the channels used for input and output streams.
// A larger channel size can improve throughput for high-volume data processing.
// Default (when unspecified) is 70 (see defaultChanSize in reader.go).
//
// Example:
//
//	clp.NewConcurrentLineProcessor(reader, clp.WithChannelSize(1000)) // 1000 items in channel
func WithChannelSize(size int) Option {
	return func(pr *concurrentLineProcessor) {
		pr.channelSize = size
	}
}

// WithReaders sets multiple source readers for the concurrentLineProcessor.
// When used, the reader passed to NewConcurrentLineProcessor can be nil because this option replaces the internal reader list.
// Empty readers will by handled by Read method.
//
// Example:
//
//	readers := []io.ReadCloser{reader1, reader2, reader3}
//	clp.NewConcurrentLineProcessor(nil, clp.WithReaders(readers...))
func WithReaders(readers ...io.ReadCloser) Option {
	return func(pr *concurrentLineProcessor) {
		pr.readers = readers
	}
}

// WithContext sets the context for the concurrentLineProcessor.
// This context can be used to manage cancellation and timeouts for the processing operations.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	clp.NewConcurrentLineProcessor(reader, clp.WithContext(ctx))
func WithContext(ctx context.Context) Option {
	return func(pr *concurrentLineProcessor) {
		pr.ctx = ctx
	}
}
