// Package concurrentlineprocessor provides a high-performance, concurrent line-by-line processor for large files or streams.
//
// This package allows you to efficiently process large files or streams by splitting the input into chunks and processing each line concurrently using multiple goroutines.
// It now supports orchestrating multiple io.ReadCloser sources as a single logical stream, allowing you to merge large datasets without custom plumbing.
//
// # Features
//   - Concurrent processing of lines using a configurable number of workers (goroutines)
//   - Custom line processor function for transforming or filtering lines
//   - Metrics reporting (bytes read, rows read, processing time, etc.)
//   - Optional row read limit
//
// # Basic Usage
//
//	import (
//	    "os"
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
//	pr := clp.NewConcurrentLineProcessor(f, clp.WithCustomLineProcessor(func(line []byte) ([]byte, error) {
//	    // Transform or filter the line
//	    return bytes.ToUpper(line), nil
//	}))
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

var (
	KB = 1024
	// defaultChunkSize is the default size for reading chunks from the source (64KB).
	// This provides a good balance between memory usage and performance for most use cases.
	defaultChunkSize = 64 * KB

	// defaultWorkers is the default number of goroutines used for processing chunks.
	// It defaults to the number of CPU cores available.
	defaultWorkers  = runtime.NumCPU()
	defaultChanSize = 70

	// maxLineLength defines the maximum length of a single line.
	// Any line longer than this will not be accepted and will result in an error.
	maxLineLength = 16 * KB
)

// NewConcurrentLineProcessor creates a new concurrentLineProcessor that reads from the provided io.ReadCloser.
// It starts processing immediately in background goroutines and returns a processor that implements io.Reader.
//
// When you need to process more than one source, pass nil as the reader and supply inputs with WithMultiReaders.
//
// The processor splits input into chunks, processes each line concurrently using multiple workers,
// and provides the processed output through the Read method.
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

		customLineProcessor: func(b []byte, _ *LineDetails) ([]byte, error) { return b, nil },
	}, opts...)

	p.lineDetailsPool = sync.Pool{New: func() any { return &LineDetails{} }}
	p.chunkPool = sync.Pool{
		New: func() any {
			return &Chunk{data: make([]byte, p.chunkSize)}
		},
	}

	p.inStream = make(chan *Chunk, p.channelSize)
	p.outStream = make(chan *Chunk, p.channelSize)

	go func() { p.start() }()
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

	return "chunkSize=" + FormatBytes(float64(p.chunkSize)) +
		" workers=" + strconv.Itoa(p.workers) +
		" channelSize=" + strconv.Itoa(p.channelSize) +
		" rowsReadLimit=" + strconv.Itoa(p.rowsReadLimit) +
		" bytesRead=" + FormatBytes(float64(metrics.BytesRead)) +
		" bytesWritten=" + FormatBytes(float64(metrics.BytesWritten)) +
		" rowsRead=" + strconv.FormatInt(metrics.RowsRead, 10) +
		" rowsWritten=" + strconv.FormatInt(metrics.RowsWritten, 10) +
		" throughput=" + FormatBytes(float64(metrics.BytesWritten)/sec) + "/s" +
		" elapsed=" + FormatDuration(metrics.TimeTook)
}

func (p *concurrentLineProcessor) start() {
	eg, ctx := errgroup.WithContext(p.ctx)
	eg.Go(func() error { return p.readAsChunks(ctx) })
	eg.Go(func() error { return p.processChunks(ctx) })
	eg.Go(func() error { return p.writeProcessedData(ctx) })

	// go PrintSummaryPeriodically(p)

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
		chunkID, linesToUpdate, rr int

		leftOver = make([]byte, 0, maxLineLength)
		currBuff = p.newChunkFromPool(-1, -1) // temporary buffer for reading
	)
	defer p.putChunkToPool(currBuff)

	for {
		if rr = p.RowsRead(); p.rowsReadLimit != -1 && rr >= p.rowsReadLimit { // If rowsReadLimit is set, check if it has been reached
			break
		}

		chunk := p.newChunkFromPool(chunkID, readerID)
		copied := moveAllData(chunk, 0, leftOver)

		read, readErr := r.Read(currBuff.data)
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return readErr
			}
			if copied > 0 {
				if err := sendToStream(ctx, p.inStream, chunk); err != nil {
					return err
				}
			}
			return nil
		}

		moveAllData(chunk, copied, currBuff.data[:read])
		chunk.endingPos, linesToUpdate = trimmedBuff(chunk.data[:chunk.endingPos], p.rowsReadLimit, rr)
		atomic.AddInt64(&p.metrics.RowsRead, int64(linesToUpdate))
		atomic.AddInt64(&p.metrics.BytesRead, int64(read))

		ind := bytes.LastIndex(chunk.data[:chunk.endingPos], []byte{'\n'})
		if ind == -1 {
			if chunk.endingPos > maxLineLength {
				return errors.New("line length exceeds maximum allowed length of " + strconv.Itoa(maxLineLength) + " bytes")
			}
			leftOver = append(leftOver[:0], chunk.data...)
			continue
		}

		if chunk.endingPos-ind > maxLineLength {
			return errors.New("line length exceeds maximum allowed length of " + strconv.Itoa(maxLineLength) + " bytes")
		}

		leftOver = append(leftOver[:0], chunk.data[ind+1:chunk.endingPos]...)
		chunk.endingPos = ind + 1
		if err := sendToStream(ctx, p.inStream, chunk); err != nil {
			return err
		}

		chunkID++
	}
	return nil
}

func (p *concurrentLineProcessor) processChunks(ctx context.Context) error {
	defer close(p.outStream)
	poolErrG, ctxEg := errgroup.WithContext(ctx)
	for range p.workers {
		poolErrG.Go(func() error {
			for {
				chunk, err := getFromStream(ctxEg, p.inStream)
				if err != nil || chunk == nil {
					return err
				}
				if err := p.processSingleChunk(ctxEg, chunk); err != nil {
					return err
				}
			}
		})
	}
	return poolErrG.Wait()
}

func (p *concurrentLineProcessor) processSingleChunk(ctx context.Context, chunk *Chunk) error {
	if !p.hasCustomLineProcessor {
		AppendNewLine(chunk)
		chunk.rowsWritten += int64(bytes.Count(chunk.data[:chunk.endingPos], []byte("\n")))
		return sendToStream(ctx, p.outStream, chunk)
	}

	var (
		lineDetails = p.lineDetailsPool.Get().(*LineDetails)
		data        = chunk.data[:chunk.endingPos]

		lineStart, ind, lineEnd int
	)

	// put the original chunk data back to the pool
	defer p.putChunkToPool(chunk)
	defer p.lineDetailsPool.Put(lineDetails)

	lineDetails.ChunkID, lineDetails.ReaderID = chunk.id, chunk.readerID
	resChunk := p.newChunkFromPool(chunk.id, chunk.readerID)

	for lineStart < len(data) {
		ind = bytes.IndexByte(data[lineStart:], '\n')
		lineEnd = lineStart + ind // the ind is relative to buff passed to IndexByte
		if ind == -1 {
			lineEnd = len(data)
		}

		pb, err := p.customLineProcessor(data[lineStart:lineEnd], lineDetails)
		if err != nil {
			p.putChunkToPool(resChunk)
			return err
		}

		moveAllData(resChunk, resChunk.endingPos, pb)
		AppendNewLine(resChunk)

		lineStart = lineEnd + 1
	}

	// Learning: writing each line to the output stream one by one drastically worse the performance
	// due to the channels getting blocked for after few single line writes
	// It is better to write the whole chunk at once to the output stream
	resChunk.rowsWritten += int64(bytes.Count(resChunk.data[:resChunk.endingPos], []byte("\n")))
	return sendToStream(ctx, p.outStream, resChunk)
}

func (p *concurrentLineProcessor) writeProcessedData(ctx context.Context) error {
	for {
		chunk, err := getFromStream(ctx, p.outStream)
		if err != nil || chunk == nil {
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

func getFromStream(ctx context.Context, ch chan *Chunk) (*Chunk, error) {
	select {
	case chunk := <-ch:
		return chunk, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func sendToStream(ctx context.Context, ch chan *Chunk, chunk *Chunk) error {
	if chunk == nil || chunk.endingPos == 0 {
		return nil
	}
	select {
	case ch <- chunk:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func trimmedBuff(buff []byte, readLimit, currLinesRead int) (int, int) {
	newLinesCnt := bytes.Count(buff, []byte{'\n'})
	linesNeeded := newLinesCnt
	if readLimit != -1 {
		linesNeeded = readLimit - currLinesRead
	}
	if linesNeeded >= newLinesCnt {
		return len(buff), newLinesCnt
	}

	if linesNeeded <= 0 {
		return 0, 0
	}

	searchArea := buff
	var linesFound, buffLen, ind int
	for {
		ind = bytes.IndexByte(searchArea, '\n')
		if ind == -1 {
			break
		}
		linesFound++
		buffLen += ind + 1
		if linesFound >= linesNeeded {
			// the buff includes new line at the end
			return buffLen, linesFound
		}
		searchArea = searchArea[ind+1:]
	}
	// If not enough newlines were found, the whole buffer is used.
	return len(buff), linesFound
}

func (p *concurrentLineProcessor) newChunkFromPool(chunkID, readerID int) *Chunk {
	chunk := p.chunkPool.Get().(*Chunk)
	// Reslicing prevents the reuse of larger buffers (in ⁠.Read) that were created by appends.
	// When a grown buffer is returned to the pool, appending makes it even larger.
	chunk.data = chunk.data[:p.chunkSize]
	chunk.id, chunk.readerID, chunk.endingPos, chunk.rowsWritten = chunkID, readerID, 0, 0
	return chunk
}

func moveAllData(chunk *Chunk, start int, src []byte) int {
	if copied := copy(chunk.data[start:], src); copied < len(src) {
		chunk.data = append(chunk.data, src[copied:]...) // append the remaining bytes
	}
	chunk.endingPos += len(src)
	return len(src)
}
