// Package concurrentlineprocessor provides a high-performance, concurrent line-by-line processor for large files or streams.
//
// This package allows you to efficiently process large files or streams by splitting the input into chunks
// and processing each line (or chunk) concurrently using multiple goroutines.
// It supports orchestrating multiple io.ReadCloser sources as a single logical stream,
// allowing you to merge large datasets without custom plumbing.
//
// # Features
//   - Concurrent processing using a configurable number of workers (goroutines)
//   - Custom line processor function for transforming or filtering individual lines
//   - Custom chunk processor function for processing entire chunks at once
//   - ChunkDetails context passed to processors with ReaderID and ChunkID
//   - Metrics reporting (bytes read/written, rows read/written, processing time)
//   - Optional row read limit for sampling or testing
//   - Multi-source input: merge multiple io.ReadCloser inputs into one stream
//   - Backpressure-friendly internal bounded channels
//   - Memory-efficient sync.Pool-based chunk allocation
//
// # Output Ordering
//
// IMPORTANT: By default, output lines may NOT preserve the original input order.
// Concurrent processing with multiple workers causes chunks to complete at different
// times, resulting in unpredictable output ordering.
//
// To guarantee ordered output:
//   - Use WithWorkers(1) to process sequentially
//   - For multiple input sources, merge them with io.MultiReader before passing to the processor
//     instead of using WithMultiReaders/WithReaders (which processes sources concurrently)
//
// # Basic Usage
//
//	import (
//	    "os"
//	    "io"
//	    clp "github.com/anvesh9652/concurrent-line-processor"
//	)
//
//	f, err := os.Open("largefile.txt")
//	clp.ExitOnError(err)
//	defer f.Close()
//	pr := clp.NewConcurrentLineProcessor(f, clp.WithWorkers(4), clp.WithChunkSize(1024*1024))
//	output, err := io.ReadAll(pr)
//	clp.ExitOnError(err)
//	fmt.Println(string(output))
//
// # Custom Line Processing
//
// The DataProcessor function signature is: func(b []byte, info *ChunkDetails, out io.Writer) error
// Processors write their output to the provided io.Writer and return any error.
//
//	pr := clp.NewConcurrentLineProcessor(f, clp.WithCustomLineProcessor(
//	    func(line []byte, info *clp.ChunkDetails, out io.Writer) error {
//	        _, err := out.Write(bytes.ToUpper(line))
//	        return err
//	    },
//	))
//
// # Custom Chunk Processing
//
// For processing entire chunks at once (e.g., aggregation):
//
//	pr := clp.NewConcurrentLineProcessor(f, clp.WithCustomChunkProcessor(
//	    func(chunk []byte, info *clp.ChunkDetails, out io.Writer) error {
//	        // Process entire chunk
//	        _, err := out.Write(chunk)
//	        return err
//	    },
//	))
//
// # Metrics
//
//	metrics := pr.Metrics()
//	fmt.Printf("Rows read: %d, Bytes read: %d, Time took: %s\n", metrics.RowsRead, metrics.BytesRead, metrics.TimeTook)
//
// For more advanced usage, see the examples/ directory.
package concurrentlineprocessor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	KB = 1024
	// defaultChunkSize is the default size for reading chunks from the source (64KB).
	// This provides a good balance between memory usage and performance for most use cases.
	defaultChunkSize = 64 * KB

	defaultChanSize = 70
)

var (
	// defaultWorkers is the default number of goroutines used for processing chunks.
	// It defaults to the number of CPU cores available to use.
	defaultWorkers = runtime.GOMAXPROCS(0)

	newLine = []byte{'\n'}
)

// NewConcurrentLineProcessor creates a new concurrentLineProcessor that reads from the provided io.ReadCloser.
// It starts processing immediately in background goroutines and returns a processor that implements io.Reader.
//
// When you need to process more than one source, pass nil as the reader and supply inputs with WithMultiReaders.
//
// The processor splits input into chunks, processes each line concurrently using multiple workers,
// and provides the processed output through the Read method.
//
// IMPORTANT: Output order is NOT guaranteed by default. Use WithWorkers(1) for ordered output.
//
// Example:
//
//	file, err := os.Open("large-file.txt")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	processor := clp.NewConcurrentLineProcessor(file,
//		clp.WithWorkers(8),
//		clp.WithChunkSize(1024*1024),
//	)
//
//	output, err := io.ReadAll(processor)
//	if err != nil {
//		log.Fatal(err)
//	}
func NewConcurrentLineProcessor(r io.ReadCloser, opts ...Option) *concurrentLineProcessor {
	pr, pw := io.Pipe()

	p := WithOpts(&concurrentLineProcessor{
		ctx:     context.Background(),
		readers: []io.ReadCloser{r},

		workers:       defaultWorkers,
		chunkSize:     defaultChunkSize,
		channelSize:   defaultChanSize,
		rowsReadLimit: -1,

		pr: pr, pw: pw,
		now: time.Now(),
	}, opts...)

	p.chunkDetailsPool = sync.Pool{New: func() any { return &ChunkDetails{} }}
	p.chunkPool = sync.Pool{
		New: func() any {
			return &Chunk{data: make([]byte, p.chunkSize)}
		},
	}

	p.inStream = make(chan *Chunk, p.channelSize)
	p.outStream = make(chan *Chunk, p.channelSize)

	go p.start()
	return p
}

// Read implements io.Reader interface, allowing the processed data to be read
// using standard Go I/O patterns like io.Copy, io.ReadAll, bufio.Scanner, etc.
func (p *concurrentLineProcessor) Read(b []byte) (int, error) {
	return p.pr.Read(b)
}

func (p *concurrentLineProcessor) Close() (retErr error) {
	for _, r := range p.readers {
		if err := r.Close(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}
	if err := p.pr.Close(); err != nil {
		retErr = errors.Join(retErr, err)
	}
	return
}

// Metrics returns the current processing metrics including bytes read, rows processed,
// and total processing time. The metrics are safe to access concurrently.
func (p *concurrentLineProcessor) Metrics() Metrics {
	return Metrics{
		RowsRead:     atomic.LoadInt64(&p.metrics.RowsRead),
		RowsWritten:  atomic.LoadInt64(&p.metrics.RowsWritten),
		BytesRead:    atomic.LoadInt64(&p.metrics.BytesRead),
		BytesWritten: atomic.LoadInt64(&p.metrics.BytesWritten),
		TimeTook:     time.Since(p.now),
	}
}

// RowsRead returns the current number of rows that have been read from the source.
func (p *concurrentLineProcessor) RowsRead() int {
	return int(atomic.LoadInt64(&p.metrics.RowsRead))
}

// Summary returns a string summarizing the settings and metrics of the processor.
// Note: time took is only updated after the processing is complete.
func (p *concurrentLineProcessor) Summary() string {
	metrics := p.Metrics()

	var sec float64 = metrics.TimeTook.Seconds()
	if sec == 0 {
		sec = 1 // to avoid division by zero
	}

	return "chunkSize=" + FormatBytes(float64(p.chunkSize), BaseBinary) +
		" workers=" + strconv.Itoa(p.workers) +
		" channelSize=" + strconv.Itoa(p.channelSize) +
		" rowsReadLimit=" + strconv.Itoa(p.rowsReadLimit) +
		" bytesRead=" + FormatBytes(float64(metrics.BytesRead), BaseSI) +
		" bytesWritten=" + FormatBytes(float64(metrics.BytesWritten), BaseSI) +
		" rowsRead=" + strconv.FormatInt(metrics.RowsRead, 10) +
		" rowsWritten=" + strconv.FormatInt(metrics.RowsWritten, 10) +
		" throughput=" + FormatBytes(float64(metrics.BytesWritten)/sec, BaseSI) + "/s" +
		" elapsed=" + FormatDuration(metrics.TimeTook)
}

func (p *concurrentLineProcessor) start() {
	eg, ctx := errgroup.WithContext(p.ctx)
	eg.Go(func() error { return p.readAsChunks(ctx) })
	eg.Go(func() error { return p.processChunks(ctx) })
	eg.Go(func() error { return p.writeProcessedData(ctx) })

	// go PrintSummaryPeriodically(ctx, p, 5*time.Second)

	// Learning: if a goroutine returns an error, and the other goroutines are still running.
	// we will not get any error on eg.Wait() if we don't use errgroup with context.
	err := eg.Wait()
	p.drainChannelData()
	// we never know when user calls .Metrics(). So we have to update the actual time took here.
	p.metrics.TimeTook = time.Since(p.now)
	p.pw.CloseWithError(err)
}

func (p *concurrentLineProcessor) readAsChunks(ctx context.Context) error {
	defer close(p.inStream)

	eg, ctx := errgroup.WithContext(ctx)
	for i, r := range p.readers {
		if r == nil {
			continue
		}
		eg.Go(func() error {
			return p.handleReader(ctx, i, r)
		})
	}
	return eg.Wait()
}

func (p *concurrentLineProcessor) handleReader(ctx context.Context, readerID int, r io.ReadCloser) error {
	var (
		chunkID, linesToUpdate int

		chunk = p.newChunkFromPool(chunkID, readerID)
	)

	for {
		if p.rowsReadLimit != -1 && p.RowsRead() >= p.rowsReadLimit { // If rowsReadLimit is set, check if it has been reached
			break
		}

		rem := min(cap(chunk.data)-chunk.endingPos, p.chunkSize)

		// If the single line it self is bigger than given chunk size then it's fine to grow the
		// chunk down the line since we didn't want to endup in an infinite loop.
		if rem == 0 {
			rem = max(p.chunkSize, 32*KB)
		}
		chunk.Grow(rem)
		read, readErr := r.Read(chunk.data[chunk.endingPos : chunk.endingPos+rem])
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return readErr
			}

			if read == 0 {
				if chunk.endingPos > 0 {
					atomic.AddInt64(&p.metrics.RowsRead, 1) // if we are here then it's the last line without "\n" at end
					return sendToStream(ctx, p.inStream, chunk)
				}
				break
			}
		}
		chunk.endingPos += read
		chunk.endingPos, linesToUpdate = p.trimmedBuff(chunk.data[:chunk.endingPos])
		atomic.AddInt64(&p.metrics.BytesRead, int64(read))

		if p.isLimitReached(chunk, linesToUpdate) {
			p.putChunkToPool(chunk)
			break
		}

		ind := bytes.LastIndex(chunk.data[:chunk.endingPos], newLine)
		if ind == -1 {
			continue
		}
		nextChunk := p.newChunkFromPool(chunkID+1, readerID)
		if ind+1 < chunk.endingPos {
			_, _ = nextChunk.Write(chunk.data[ind+1 : chunk.endingPos])
		}

		chunk.endingPos = ind + 1
		if err := sendToStream(ctx, p.inStream, chunk); err != nil {
			return err
		}

		chunkID++
		chunk = nextChunk
	}
	return nil
}

func (p *concurrentLineProcessor) processChunks(ctx context.Context) error {
	defer close(p.outStream)
	poolErrG, ctxEg := errgroup.WithContext(ctx)
	for range p.workers {
		poolErrG.Go(func() error {
			for chunk := range p.inStream {
				if err := ctxEg.Err(); err != nil {
					return err
				}
				if err := p.processSingleChunk(ctxEg, chunk); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return poolErrG.Wait()
}

func (p *concurrentLineProcessor) processSingleChunk(ctx context.Context, chunk *Chunk) error {
	if p.isLineProcessor == nil || p.customDataProcessor == nil {
		EnsureNewLineAtEnd(chunk)
		chunk.rowsWritten += int64(bytes.Count(chunk.data[:chunk.endingPos], []byte("\n")))
		return sendToStream(ctx, p.outStream, chunk)
	}

	var (
		chunkDetails = p.chunkDetailsPool.Get().(*ChunkDetails)
		data         = chunk.data[:chunk.endingPos]
	)

	// put the original chunk data back to the pool
	defer p.putChunkToPool(chunk)
	defer p.chunkDetailsPool.Put(chunkDetails)

	chunkDetails.ChunkID, chunkDetails.ReaderID = chunk.id, chunk.readerID
	resChunk := p.newChunkFromPool(chunk.id, chunk.readerID)

	if !*p.isLineProcessor {
		if err := p.customDataProcessor(data, chunkDetails, resChunk); err != nil {
			p.putChunkToPool(resChunk)
			return err
		}
		EnsureNewLineAtEnd(resChunk)
	} else {
		for line := range Lines(chunk.data[:chunk.endingPos], false) {
			if err := p.customDataProcessor(line, chunkDetails, resChunk); err != nil {
				p.putChunkToPool(resChunk)
				return err
			}
			EnsureNewLineAtEnd(resChunk)
		}
	}

	// Learning: writing each line to the output stream one by one drastically worse the performance
	// due to the channels getting blocked for after few single line writes
	// It is better to write the whole chunk at once to the output stream
	resChunk.rowsWritten += int64(bytes.Count(resChunk.data[:resChunk.endingPos], []byte("\n")))
	return sendToStream(ctx, p.outStream, resChunk)
}

func (p *concurrentLineProcessor) writeProcessedData(ctx context.Context) error {
	for chunk := range p.outStream {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Inline function to safely put bufferes back into the pool after writing
		write := func(chunk *Chunk) error {
			defer p.putChunkToPool(chunk)
			n, err := p.pw.Write(chunk.data[:chunk.endingPos])
			if err != nil {
				return err
			}

			atomic.AddInt64(&p.metrics.BytesWritten, int64(n))
			atomic.AddInt64(&p.metrics.RowsWritten, chunk.rowsWritten)
			return nil
		}
		if err := write(chunk); err != nil {
			return err
		}
	}
	return nil
}

// drainChannelData drains the input and output channels to ensure no data is leaking after any errors
func (p *concurrentLineProcessor) drainChannelData() {
	for chunk := range p.inStream {
		p.chunkPool.Put(chunk)
	}
	for chunk := range p.outStream {
		p.chunkPool.Put(chunk)
	}
}

func (p *concurrentLineProcessor) putChunkToPool(chunk *Chunk) {
	if chunk == nil {
		return
	}
	p.chunkPool.Put(chunk)
}

func sendToStream(ctx context.Context, ch chan *Chunk, chunk *Chunk) error {
	if chunk == nil || chunk.endingPos == 0 {
		return nil
	}

	// FAST PATH: Try to send immediately.
	select {
	case ch <- chunk:
		return nil
	default:
		// SLOW PATH: Channel is full.
		select {
		case ch <- chunk:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *concurrentLineProcessor) trimmedBuff(buff []byte) (int, int) {
	buffLinesCnt := bytes.Count(buff, newLine)
	if p.rowsReadLimit == -1 {
		return len(buff), buffLinesCnt
	}

	currLinesRead, linesNeeded := p.RowsRead(), buffLinesCnt
	if p.rowsReadLimit != -1 {
		linesNeeded = p.rowsReadLimit - currLinesRead
	}

	// Entire buff it self doesn't have enough lines we need
	if linesNeeded >= buffLinesCnt {
		return len(buff), buffLinesCnt
	}

	// We already read the row till limit. we no longer need anything from this buff
	if linesNeeded <= 0 {
		return 0, 0
	}

	var linesFound, buffLen int
	for line := range Lines(buff, true) {
		linesFound++
		buffLen += len(line)

		if linesFound >= linesNeeded {
			break
		}
	}
	return buffLen, linesFound
}

func (p *concurrentLineProcessor) newChunkFromPool(chunkID, readerID int) *Chunk {
	chunk := p.chunkPool.Get().(*Chunk)
	// Reslicing prevents the reuse of larger buffers (in ⁠.Read) that were created by appends.
	// When a grown buffer is returned to the pool, appending makes it even larger.
	chunk.data = chunk.data[:p.chunkSize]
	chunk.id, chunk.readerID, chunk.endingPos, chunk.rowsWritten = chunkID, readerID, 0, 0
	return chunk
}

// Tells whether to proceed or not upon reaching limit
func (p *concurrentLineProcessor) isLimitReached(chunk *Chunk, linesToUpdate int) bool {
	val := int(atomic.AddInt64(&p.metrics.RowsRead, int64(linesToUpdate)))
	if p.rowsReadLimit == -1 || val <= p.rowsReadLimit {
		return false
	}

	if val-linesToUpdate >= p.rowsReadLimit {
		atomic.AddInt64(&p.metrics.RowsRead, -int64(linesToUpdate))
		return true
	}
	// Here we kind of have overlap and
	// we need to remove excess lines, which might have been added by other readHandler
	var till, c int
	for line := range Lines(chunk.data[:chunk.endingPos], true) {
		till, c = till+len(line), c+1
		if c == linesToUpdate-(val-p.rowsReadLimit) {
			break
		}
	}

	atomic.SwapInt64(&p.metrics.RowsRead, int64(p.rowsReadLimit))
	chunk.endingPos = till
	return false
}
