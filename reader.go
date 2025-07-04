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
//	clp.ExistOnError(err)
//	defer f.Close()
//	pr := clp.NewConcurrentLineProcessor(f, clp.WithWorkers(4), clp.WithChunkSize(1024*1024))
//	output, err := io.ReadAll(pr)
//	clp.ExistOnError(err)
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
	DefaultChunkSize = 1024 * 30 // 30 KB

	// DefaultWorkers is the number of goroutines used for processing chunks
	DefaultWorkers  = runtime.NumCPU()
	defaultChanSize = 50
)

// NewConcurrentLineProcessor creates a new ConcurrentLineProcessor that reads from the provided io.Reader.
func NewConcurrentLineProcessor(r io.Reader, opts ...Option) *ConcurrentLineProcessor {
	pr, pw := io.Pipe()
	p := &ConcurrentLineProcessor{
		srcReader: r,

		workers:       DefaultWorkers,
		chunkSize:     DefaultChunkSize,
		rowsReadLimit: -1,

		inStream:  make(chan *Chunk, defaultChanSize),
		outStream: make(chan *Chunk, defaultChanSize),

		pool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, defaultChanSize)
				return &b
			},
		},

		pr: pr, pw: pw,

		customLineProcessor: func(b []byte) ([]byte, error) { return b, nil },
	}

	WithOpts(p, opts...)
	go func() { p.start() }()
	return p
}

func (p *ConcurrentLineProcessor) Read(b []byte) (int, error) {
	return p.pr.Read(b)
}

func (p *ConcurrentLineProcessor) Metrics() Metrics {
	return p.metrics
}

func (p *ConcurrentLineProcessor) RowsRead() int {
	return int(atomic.LoadInt64(&p.metrics.RowsRead))
}

func (p *ConcurrentLineProcessor) start() {
	now := time.Now()
	eg, ctx := errgroup.WithContext(context.Background())
	eg.Go(func() error { return p.readAsChunks(ctx) })
	eg.Go(func() error { return p.processChunks(ctx) })
	eg.Go(func() error { return p.readProcessedData(ctx) })

	// Learning: if a goroutine returns an error, the other goroutines are still running.
	// Therefore, we will not get any error on eg.Wait()
	err := eg.Wait()
	p.metrics.TimeTook = time.Since(now).String()
	p.pw.CloseWithError(err)
}

func (p *ConcurrentLineProcessor) readAsChunks(ctx context.Context) error {
	var (
		leftOver = make([]byte, 0, p.chunkSize*2)
		buff     = make([]byte, p.chunkSize)
		chunkID  = 1
	)

	defer close(p.inStream)
	for {
		// If rowsReadLimit is set, check if it has been reached
		if p.rowsReadLimit != -1 && p.RowsRead() >= p.rowsReadLimit {
			break
		}
		read, err := p.srcReader.Read(buff)
		p.metrics.BytesRead += int64(read)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(leftOver) != 0 {
					if err := sendToStream(ctx, p.inStream, &Chunk{id: chunkID, data: &leftOver}); err != nil {
						return err
					}
				}
				break
			}
			return err
		}

		currBuff := buff[:read]
		ind := bytes.LastIndex(currBuff, []byte("\n"))
		if ind == -1 {
			leftOver = append(leftOver, buff[:read]...)
			continue
		}

		leftOver = append(leftOver, currBuff[:ind]...)
		copyToSend := make([]byte, len(leftOver))
		copy(copyToSend, leftOver)

		leftOver = append(leftOver[:0], currBuff[ind+1:]...)

		if err := sendToStream(ctx, p.inStream, &Chunk{id: chunkID, data: &copyToSend}); err != nil {
			return err
		}

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
					return err
				}
			}
		})
	}
	return poolErrG.Wait()
}

func (p *ConcurrentLineProcessor) processChunk(ctx context.Context, chunk *Chunk) error {
	lineStart := 0

	buff := p.pool.Get().(*[]byte)
	data := *chunk.data
	for i, r := range data {
		if i == len(data)-1 {
			i++
		}
		if r == '\n' || i == len(data) {
			pb, err := p.customLineProcessor(data[lineStart:i])
			if err != nil {
				return err
			}
			// Learning: writing each line to the output stream one by one drastically worse performance
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
		newLines := bytes.Count(*buff, []byte("\n"))
		rr := int(p.RowsRead())
		linesNeeded := newLines
		if p.rowsReadLimit != -1 {
			if rr+newLines > p.rowsReadLimit {
				linesNeeded = (p.rowsReadLimit - rr)
				pos := ithBytePosition(buff, linesNeeded, '\n')
				*buff = (*buff)[:pos+1] // +1 to include the new line character
			}
		}

		n, err := p.pw.Write(*buff)
		if err != nil {
			return err
		}

		// Actually, it's not necessary to count the bytes atomically.
		// This was done to observe how many bytes are read at each tick.
		atomic.AddInt64(&p.metrics.RowsRead, int64(linesNeeded))
		atomic.AddInt64(&p.metrics.TransformedBytes, int64(n))
		*buff = (*buff)[:0]
		p.pool.Put(buff)

		if p.rowsReadLimit != -1 && p.RowsRead() >= p.rowsReadLimit {
			break
		}
	}
	return nil
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
