// Package concurrentlineprocessor provides a high-performance, concurrent line-by-line processor for large files or streams.
//
// See reader.go for full package documentation and usage examples.
package concurrentlineprocessor

// WithOpts applies the given options to the concurrentLineProcessor.
// This is a convenience function for applying multiple options at once.
func WithOpts(p *concurrentLineProcessor, opts ...Option) {
	for _, opt := range opts {
		opt(p)
	}
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

// WithCustomLineProcessor sets a custom function to process each line.
// The function receives a line as []byte and should return the processed line.
// The function must be thread-safe and should not modify external state
// without proper synchronization.
//
// Example:
//
//	// Convert lines to uppercase
//	processor := func(line []byte) ([]byte, error) {
//	    return bytes.ToUpper(line), nil
//	}
//	clp.NewConcurrentLineProcessor(reader, clp.WithCustomLineProcessor(processor))
func WithCustomLineProcessor(c LineProcessor) Option {
	return func(pr *concurrentLineProcessor) {
		pr.customLineProcessor = c
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
// A larger channel size can improve throughput for high-volume data processing,
// The default channel size is 100.
//
// Example:
//
//	clp.NewConcurrentLineProcessor(reader, clp.WithChannelSize(1000)) // 1000 items in channel
func WithChannelSize(size int) Option {
	return func(pr *concurrentLineProcessor) {
		pr.channelSize = size
	}
}
