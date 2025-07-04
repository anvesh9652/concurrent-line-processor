package main

func WithOpts(p *ParallelReader, opts ...Option) {
	for _, opt := range opts {
		opt(p)
	}
}

func WithChunkSize(size int) Option {
	return func(pr *ParallelReader) {
		pr.chunkSize = size
	}
}

func WithWorkers(n int) Option {
	return func(pr *ParallelReader) {
		pr.workers = n
	}
}

func WithCustomLineProcessor(c LineProcessor) Option {
	return func(pr *ParallelReader) {
		pr.customLineProcessor = c
	}
}

func WithRowsReadLimit(limit int) Option {
	return func(pr *ParallelReader) {
		pr.rowsReadLimit = limit
	}
}
