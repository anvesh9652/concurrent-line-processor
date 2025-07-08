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
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// TODO:
// 	- Need to reduce the number of memory allocations
//  - Probably provide chunk id and row id to the custom line processor
//  - Option to skip first n lines

var (
	// DefaultChunkSize is the default size for reading chunks from the source (30KB).
	// This provides a good balance between memory usage and performance for most use cases.
	DefaultChunkSize = 1024 * 30 // 30 KB

	// DefaultWorkers is the default number of goroutines used for processing chunks.
	// It defaults to the number of CPU cores available.
	DefaultWorkers  = runtime.NumCPU()
	defaultChanSize = 50
)

// NewConcurrentLineProcessor creates a new ConcurrentLineProcessor that reads from the provided io.Reader.
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
func NewConcurrentLineProcessor(r io.Reader, opts ...Option) *ConcurrentLineProcessor {
	pr, pw := io.Pipe()
	p := &ConcurrentLineProcessor{
		srcReader: r,

		workers:       DefaultWorkers,
		chunkSize:     DefaultChunkSize,
		rowsReadLimit: -1,

		inStream:  make(chan *Chunk, defaultChanSize),
		outStream: make(chan *Chunk, defaultChanSize),

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

	go func() { p.start() }()
	return p
}

// Read implements io.Reader interface, allowing the processed data to be read
// using standard Go I/O patterns like io.Copy, io.ReadAll, bufio.Scanner, etc.
func (p *ConcurrentLineProcessor) Read(b []byte) (int, error) {
	return p.pr.Read(b)
}

// Metrics returns the current processing metrics including bytes read, rows processed,
// and total processing time. The metrics are safe to access concurrently.
func (p *ConcurrentLineProcessor) Metrics() Metrics {
	return Metrics{
		RowsRead:         atomic.LoadInt64(&p.metrics.RowsRead),
		BytesRead:        atomic.LoadInt64(&p.metrics.BytesRead),
		TransformedBytes: atomic.LoadInt64(&p.metrics.TransformedBytes),
		TimeTook:         p.metrics.TimeTook, // It's safe to return --race flag isn't complaining
	}
}

// RowsRead returns the current number of rows that have been read from the source.
// This value is updated atomically and is safe to call concurrently.
func (p *ConcurrentLineProcessor) RowsRead() int {
	return int(atomic.LoadInt64(&p.metrics.RowsRead))
}

func (p *ConcurrentLineProcessor) start() {
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

func (p *ConcurrentLineProcessor) readAsChunks(ctx context.Context) error {
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

func (p *ConcurrentLineProcessor) processChunks(ctx context.Context) error {
	defer close(p.outStream)
	poolErrG, ctxEg := errgroup.WithContext(ctx)
	for range p.workers {
		poolErrG.Go(func() error {
			for {
				chunk, err := getFromStream(ctxEg, p.inStream)
				if err != nil {
					return err
				}
				if chunk == nil {
					return nil
				}
				if err := p.processChunk(ctxEg, chunk); err != nil {
					p.putBuffToPool(chunk.data)
					return err
				}
				p.putBuffToPool(chunk.data)
			}
		})
	}
	return poolErrG.Wait()
}

func (p *ConcurrentLineProcessor) processChunk(ctx context.Context, chunk *Chunk) error {
	var (
		lineStart = 0

		buff = p.pool.Get().(*[]byte)
		data = *chunk.data
	)

	for i, r := range data {
		if i == len(data)-1 {
			i++
		}
		if r == '\n' || i == len(data) {
			pb, err := p.customLineProcessor(data[lineStart:i])
			if err != nil {
				p.putBuffToPool(buff) // safety call to avoid memory leak
				return err
			}
			// Learning: writing each line to the output stream one by one drastically worse the performance
			// due to the number of system calls. It is better to write the whole chunk at once to the output stream
			*buff = append(*buff, WithNewLine(pb)...)
			lineStart = i + 1
		}
	}
	return sendToStream(ctx, p.outStream, &Chunk{id: chunk.id, data: buff})
}

func (p *ConcurrentLineProcessor) readProcessedData(ctx context.Context) error {
	for {
		chunk, err := getFromStream(ctx, p.outStream)
		if err != nil {
			return err
		}
		if chunk == nil {
			break
		}

		buff := chunk.data
		n, err := p.pw.Write(*buff)
		if err != nil {
			// Learning: we can do this in defer also, but it allocates a small amount of memory,
			// resutling lots of memory allocations for bigger files
			p.putBuffToPool(buff)
			return err
		}

		atomic.AddInt64(&p.metrics.TransformedBytes, int64(n))
		p.putBuffToPool(buff)
	}
	return nil
}

func (p *ConcurrentLineProcessor) putBuffToPool(buff *[]byte) {
	// Reset the buffer to avoid any data curruption
	*buff = (*buff)[:0]
	p.pool.Put(buff)
}

// takes 1-based position ith of the byte in the data
func ithBytePosition(data *[]byte, ith int, tar byte) int {
	for i, b := range *data {
		if b == tar {
			ith--
			if ith == 0 {
				return i
			}
		}
	}
	return -1
}

func sendToStream(ctx context.Context, s chan *Chunk, c *Chunk) error {
	select {
	case s <- c:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func getFromStream(ctx context.Context, s chan *Chunk) (*Chunk, error) {
	select {
	case chunk := <-s:
		return chunk, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func trimmedBuff(buff []byte, readLimit, currLinesRead int) ([]byte, int) {
	lines := bytes.Count(buff, []byte{'\n'})
	linesNeeded := lines
	if readLimit != -1 {
		if currLinesRead+lines >= readLimit {
			linesNeeded = (readLimit - currLinesRead)
			pos := ithBytePosition(&buff, linesNeeded, '\n')
			buff = buff[:pos+1] // +1 to include the new line character
		}
	}
	return buff, linesNeeded
}
