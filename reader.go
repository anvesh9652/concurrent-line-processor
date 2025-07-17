// Package concurrentlineprocessor provides a high-performance, concurrent line-by-line processor for large files or streams.
//
// This package allows you to efficiently process large files or streams by splitting the input into chunks and processing each line concurrently using multiple goroutines.
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

// NewConcurrentLineProcessor creates a new concurrentLineProcessor that reads from the provided io.Reader.
// It starts processing immediately in background goroutines and returns a processor that implements io.Reader.
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
func NewConcurrentLineProcessor(r io.Reader, opts ...Option) *concurrentLineProcessor {
	pr, pw := io.Pipe()
	p := &concurrentLineProcessor{
		srcReader: r,

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
	eg.Go(func() error { return p.readProcessedData(ctx) })

	// go func() {
	// 	t := time.NewTicker(5 * time.Second)
	// 	for range t.C {
	// 		fmt.Println("rows read:", p.RowsRead(), "time took so far:", time.Since(now))
	// 	}
	// }()

	// Learning: if a goroutine returns an error, and the other goroutines are still running.
	// we will not get any error on eg.Wait() if we don't use errgroup with context.
	err := eg.Wait()
	p.metrics.TimeTook = time.Since(now).String()
	p.pw.CloseWithError(err)
}

func (p *concurrentLineProcessor) readAsChunks(ctx context.Context) error {
	var (
		leftOver = p.pool.Get().(*[]byte)
		buff     = make([]byte, p.chunkSize)
		chunkID  = 1

		copyToSend *[]byte
	)

	defer p.putBuffToPool(leftOver)
	defer close(p.inStream)

	for {
		rr := p.RowsRead()
		if p.rowsReadLimit != -1 && rr >= p.rowsReadLimit { // If rowsReadLimit is set, check if it has been reached
			break
		}

		copyToSend = p.pool.Get().(*[]byte)
		read, err := p.srcReader.Read(buff)
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
					p.putBuffToPool(chunk.data)
					return err
				}
				p.putBuffToPool(chunk.data)
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

	var ind, lineEnd int
	for lineStart < len(data) {
		ind = bytes.IndexByte(data[lineStart:], '\n')
		lineEnd = lineStart + ind // the ind is relative to buff passed to IndexByte
		if ind == -1 {
			lineEnd = len(data)
		}

		pb, err := p.customLineProcessor(data[lineStart:lineEnd])
		if err != nil {
			p.putBuffToPool(buff) // safety call to avoid memory leaks
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

func (p *concurrentLineProcessor) readProcessedData(ctx context.Context) error {
	for {
		chunk, err := getFromStream(ctx, p.outStream)
		if err != nil || chunk == nil {
			return err
		}

		buff := chunk.data
		n, err := p.pw.Write(*buff)
		if err != nil {
			// Learning: we can do this in defer also, but it allocates a small amount of memory,
			// resutling lots of memory allocations for bigger files
			p.putBuffToPool(buff)
			return err
		}

		atomic.AddInt64(&p.metrics.BytesTransformed, int64(n))
		atomic.AddInt64(&p.metrics.RowsWritten, int64(bytes.Count(*buff, []byte{'\n'})))
		p.putBuffToPool(buff)
	}
}

func (p *concurrentLineProcessor) putBuffToPool(buff *[]byte) {
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
	if readLimit == -1 {
		return buff, bytes.Count(buff, []byte{'\n'})
	}

	linesNeeded := readLimit - currLinesRead
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
