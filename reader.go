package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	DefaultChunkSize = 1024 * 30 // 30 KB

	// DefaultWorkers is the number of goroutines to use for processing chunks
	DefaultWorkers  = runtime.NumCPU()
	defaultChanSize = 50

	errTest = errors.New("this is test error")
)

// NewReader creates parallel reader
// use one worker to process it sequntially
func NewReader(r io.Reader, opts ...Option) *ParallelReader {
	pr, pw := io.Pipe()
	p := &ParallelReader{
		srcReader:     r,
		workers:       DefaultWorkers,
		chunkSize:     DefaultChunkSize,
		rowsReadLimit: -1,

		inStream:  make(chan *Chunk, defaultChanSize),
		outStream: make(chan *Chunk, defaultChanSize*1000),

		pool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, defaultChanSize)
				return &b
			},
		},

		pr: pr, pw: pw,

		customLineProcessor: func(b []byte) ([]byte, error) { return b, nil },
	}

	for _, opt := range opts {
		opt(p)
	}
	go func() {
		p.start()
	}()
	return p
}

func (p *ParallelReader) start() {
	eg, ctx := errgroup.WithContext(context.Background())
	eg.Go(func() error { return p.readAsChunks(ctx) })
	eg.Go(func() error { return p.processChunks(ctx) })
	eg.Go(func() error { return p.readProcessedData(ctx) })
	go func() {
		t := time.NewTicker(5 * time.Second)
		start := time.Now()
		for range t.C {
			fmt.Println("Rows read so far:", p.RowsRead(), "Took:", time.Since(start))
		}
	}()

	p.pw.CloseWithError(eg.Wait())
}

func (p *ParallelReader) Read(b []byte) (int, error) {
	return p.pr.Read(b)
}

func (p *ParallelReader) readAsChunks(ctx context.Context) error {
	var (
		leftOver = make([]byte, 0, p.chunkSize*2)
		buff     = make([]byte, p.chunkSize)
		chunkID  = 1
	)
	defer close(p.inStream)

	for {
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

func (p *ParallelReader) processChunks(ctx context.Context) error {
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

func (p *ParallelReader) processChunk(ctx context.Context, chunk *Chunk) error {
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
			*buff = append(*buff, WithNewLine(pb)...)
			lineStart = i + 1
		}
	}
	return sendToStream(ctx, p.outStream, &Chunk{id: chunk.id, data: buff})
}

func (p *ParallelReader) readProcessedData(ctx context.Context) error {
	for {
		chunk, err := getFromStream(ctx, p.outStream)
		if err != nil {
			return err
		}
		if chunk == nil {
			break
		}

		buff := chunk.data
		atomic.AddInt64(&p.metrics.RowsRead, int64(bytes.Count(*buff, []byte("\n"))))
		atomic.AddInt64(&p.metrics.TransformedBytes, int64(len(*buff)))

		if _, err := p.pw.Write(*buff); err != nil {
			return err
		}

		*buff = (*buff)[:0]
		p.pool.Put(buff)
		if p.rowsReadLimit != -1 && p.RowsRead() >= p.rowsReadLimit {
			break
		}
	}
	return nil
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

func (p *ParallelReader) Metrics() Metrics {
	return p.metrics
}

func (p *ParallelReader) RowsRead() int {
	return int(atomic.LoadInt64(&p.metrics.RowsRead))
}
