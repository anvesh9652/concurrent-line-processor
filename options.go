package main

// WithOpts applies the given options to the ParallelReader.
func WithOpts(p *ParallelReader, opts ...Option) {
	for _, opt := range opts {
		opt(p)
	}
}

// WithChunkSize sets the chunk size for the ParallelReader.
func WithChunkSize(size int) Option {
	return func(pr *ParallelReader) {
		pr.chunkSize = size
	}
}

// WithWorkers sets the number of workers for the ParallelReader.
func WithWorkers(n int) Option {
	return func(pr *ParallelReader) {
		pr.workers = n
	}
}

// WithCustomLineProcessor sets a custom line processor for the ParallelReader.
func WithCustomLineProcessor(c LineProcessor) Option {
	return func(pr *ParallelReader) {
		pr.customLineProcessor = c
	}
}

// WithRowsReadLimit sets the row read limit for the ParallelReader.
func WithRowsReadLimit(limit int) Option {
	return func(pr *ParallelReader) {
		pr.rowsReadLimit = limit
	}
}
