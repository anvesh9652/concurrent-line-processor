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
	// defaultChunkSize is the default size for reading chunks from the source (64KB).
	// This provides a good balance between memory usage and performance for most use cases.
	defaultChunkSize = 1024 * 64 // 64 KB

	// defaultWorkers is the default number of goroutines used for processing chunks.
	// It defaults to the number of CPU cores available.
	defaultWorkers  = runtime.NumCPU()
	defaultChanSize = 70
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
	p := &concurrentLineProcessor{
		readers: []io.ReadCloser{r},

		workers:       defaultWorkers,
		chunkSize:     defaultChunkSize,
		channelSize:   defaultChanSize,
		rowsReadLimit: -1,

		pr: pr, pw: pw,

		customLineProcessor: func(b []byte) ([]byte, error) { return b, nil },
	}

	WithOpts(p, opts...)

	p.pool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, p.chunkSize)
			return &b
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

func (p *concurrentLineProcessor) Close() error {
	var firstErr error
	var once sync.Once

	for _, r := range p.readers {
		err := r.Close()
		if err != nil {
			once.Do(func() {
				firstErr = err
			})
		}
	}
	return firstErr
}

// Metrics returns the current processing metrics including bytes read, rows processed,
// and total processing time. The metrics are safe to access concurrently.
func (p *concurrentLineProcessor) Metrics() Metrics {
	return Metrics{
		RowsRead:         atomic.LoadInt64(&p.metrics.RowsRead),
		RowsWritten:      atomic.LoadInt64(&p.metrics.RowsWritten),
		BytesRead:        atomic.LoadInt64(&p.metrics.BytesRead),
		BytesTransformed: atomic.LoadInt64(&p.metrics.BytesTransformed),
		TimeTook:         p.metrics.TimeTook, // It's safe to return --race flag isn't complaining
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
	return "chunkSize=" + FormatBytes(p.chunkSize) +
		", workers=" + strconv.Itoa(p.workers) +
		", channelSize=" + strconv.Itoa(p.channelSize) +
		", rowsReadLimit=" + strconv.Itoa(p.rowsReadLimit) +
		", bytesRead=" + FormatBytes(int(metrics.BytesRead)) +
		", bytesTransformed=" + FormatBytes(int(metrics.BytesTransformed)) +
		", rowsRead=" + strconv.FormatInt(metrics.RowsRead, 10) +
		", rowsWritten=" + strconv.FormatInt(metrics.RowsWritten, 10) +
		", timeTook=" + metrics.TimeTook
}

func (p *concurrentLineProcessor) start() {
	now := time.Now()
	eg, ctx := errgroup.WithContext(context.Background())
	eg.Go(func() error { return p.readAsChunks(ctx) })
	eg.Go(func() error { return p.processChunks(ctx) })
	eg.Go(func() error { return p.writeProcessedData(ctx) })

	// go PrintSummaryPeriodically(p, now)

	// Learning: if a goroutine returns an error, and the other goroutines are still running.
	// we will not get any error on eg.Wait() if we don't use errgroup with context.
	err := eg.Wait()
	p.drainChannelData()
	p.metrics.TimeTook = time.Since(now).String()
	p.pw.CloseWithError(err)
}

func (p *concurrentLineProcessor) readAsChunks(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	defer close(p.inStream)

	for _, r := range p.readers {
		if r == nil {
			continue
		}
		eg.Go(func() error {
			var (
				leftOver = p.pool.Get().(*[]byte)
				buff     = make([]byte, p.chunkSize)
				chunkID  = 1

				copyToSend *[]byte
			)

			defer p.putBuffToPool(leftOver)

			for {
				rr := p.RowsRead()
				if p.rowsReadLimit != -1 && rr >= p.rowsReadLimit { // If rowsReadLimit is set, check if it has been reached
					break
				}

				copyToSend = p.pool.Get().(*[]byte)
				read, err := r.Read(buff)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if len(*leftOver) != 0 {
							*copyToSend = append(*copyToSend, *leftOver...)
							if err := sendToStream(ctx, p.inStream, &Chunk{id: chunkID, data: copyToSend}); err != nil {
								return err
							}
						}
						break
					}
					return err
				}

				currBuff, linesToUpdate := trimmedBuff(buff[:read], p.rowsReadLimit, rr)
				atomic.AddInt64(&p.metrics.RowsRead, int64(linesToUpdate))
				atomic.AddInt64(&p.metrics.BytesRead, int64(len(currBuff)))

				ind := bytes.LastIndex(currBuff, []byte{'\n'})
				if ind == -1 {
					*leftOver = append(*leftOver, buff[:read]...)
					continue
				}

				*leftOver = append(*leftOver, currBuff[:ind]...)
				*copyToSend = append(*copyToSend, *leftOver...)
				if err := sendToStream(ctx, p.inStream, &Chunk{id: chunkID, data: copyToSend}); err != nil {
					return err
				}

				*leftOver = append((*leftOver)[:0], currBuff[ind+1:]...)
				chunkID++
			}
			return nil
		})
	}
	return eg.Wait()
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
	var (
		lineStart = 0

		buff = p.pool.Get().(*[]byte)
		data = *chunk.data
	)
	// put the original chunk data back to the pool
	defer p.putBuffToPool(chunk.data)

	if !p.hasCustomLineProcessor {
		*buff = append(*buff, data...)
		AppendNewLine(buff)
		return sendToStream(ctx, p.outStream, &Chunk{id: chunk.id, data: buff})
	}

	var ind, lineEnd int
	for lineStart < len(data) {
		ind = bytes.IndexByte(data[lineStart:], '\n')
		lineEnd = lineStart + ind // the ind is relative to buff passed to IndexByte
		if ind == -1 {
			lineEnd = len(data)
		}

		pb, err := p.customLineProcessor(data[lineStart:lineEnd])
		if err != nil {
			p.putBuffToPool(buff)
			return err
		}
		// Learning: writing each line to the output stream one by one drastically worse the performance
		// due to the number of system calls. It is better to write the whole chunk at once to the output stream
		*buff = append(*buff, pb...)
		AppendNewLine(buff)
		lineStart = lineEnd + 1
	}

	return sendToStream(ctx, p.outStream, &Chunk{id: chunk.id, data: buff})
}

func (p *concurrentLineProcessor) writeProcessedData(ctx context.Context) error {
	for {
		chunk, err := getFromStream(ctx, p.outStream)
		if err != nil || chunk == nil {
			return err
		}

		// Inline function to safely put bufferes back into the pool after writing
		write := func(buff *[]byte) error {
			defer p.putBuffToPool(buff)
			n, err := p.pw.Write(*buff)
			if err != nil {
				return err
			}

			atomic.AddInt64(&p.metrics.BytesTransformed, int64(n))
			atomic.AddInt64(&p.metrics.RowsWritten, int64(bytes.Count(*buff, []byte{'\n'})))
			return nil
		}
		if err := write(chunk.data); err != nil {
			return err
		}
	}
}

// drainChannelData drains the input and output channels to ensure no data is leaking after any errors
func (p *concurrentLineProcessor) drainChannelData() {
	for chunk := range p.inStream {
		p.putBuffToPool(chunk.data)
	}
	for chunk := range p.outStream {
		p.putBuffToPool(chunk.data)
	}
}

func (p *concurrentLineProcessor) putBuffToPool(buff *[]byte) {
	if buff == nil {
		return
	}
	// Reset the buffer to avoid any data curruption
	*buff = (*buff)[:0]
	p.pool.Put(buff)
}

func getFromStream(ctx context.Context, s chan *Chunk) (*Chunk, error) {
	select {
	case chunk := <-s:
		return chunk, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func sendToStream(ctx context.Context, s chan *Chunk, c *Chunk) error {
	select {
	case s <- c:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func trimmedBuff(buff []byte, readLimit, currLinesRead int) ([]byte, int) {
	newLinesCnt := bytes.Count(buff, []byte{'\n'})
	linesNeeded := newLinesCnt
	if readLimit != -1 {
		linesNeeded = readLimit - currLinesRead
	}
	if linesNeeded >= newLinesCnt {
		return buff, newLinesCnt
	}

	if linesNeeded <= 0 {
		return buff[:0], 0
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
			return buff[:buffLen], linesFound
		}
		searchArea = searchArea[ind+1:]
	}
	// If not enough newlines were found, the whole buffer is used.
	return buff, linesFound
}
