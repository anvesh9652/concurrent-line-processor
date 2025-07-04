package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	DefaultChunkSize = 1024 * 30 // 30 KB

	// DefaultWorkers is the number of goroutines to use for processing chunks
	DefaultWorkers  = runtime.NumCPU()
	defaultChanSize = 50
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

		pr: pr,
		pw: pw,

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
	eg := errgroup.Group{}
	eg.Go(p.readAsChunks)
	eg.Go(p.processChunks)
	eg.Go(p.readProcessedData)
	go func() {
		t := time.NewTicker(5 * time.Second)
		start := time.Now()
		for range t.C {
			fmt.Println("Rows read so far:", p.RowsRead(), "Took:", time.Since(start))
		}
	}()

	if err := eg.Wait(); err != nil {
		p.pw.CloseWithError(err)
	}
}

func (p *ParallelReader) Read(b []byte) (int, error) {
	return p.pr.Read(b)
}

func (p *ParallelReader) readAsChunks() error {
	var (
		leftOver = make([]byte, 0, p.chunkSize*2)
		buff     = make([]byte, p.chunkSize)
		chunkID  = 1
	)
	defer close(p.inStream)
	for {
		read, err := p.srcReader.Read(buff)
		p.Metrics.BytesRead += read
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(leftOver) != 0 {
					p.inStream <- &Chunk{id: chunkID, data: &leftOver}
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

		p.inStream <- &Chunk{id: chunkID, data: &copyToSend}
		chunkID++
	}
	return nil
}

func (p *ParallelReader) processChunks() error {
	defer close(p.outStream)
	poolErrG := errgroup.Group{}
	for range p.workers + 2 {
		poolErrG.Go(func() error {
			for chunk := range p.inStream {
				if err := p.processChunk(chunk); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return poolErrG.Wait()
}

func (p *ParallelReader) processChunk(chunk *Chunk) error {
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
	p.sendToStream(&Chunk{id: chunk.id, data: buff})
	return nil
}

func (p *ParallelReader) readProcessedData() error {
	defer p.pw.Close()
	for chunk := range p.outStream {
		buff := chunk.data
		p.Metrics.RowsRead += int64(bytes.Count(*buff, []byte("\n")))
		p.Metrics.TransformedBytes += len(*buff)

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

func (p *ParallelReader) sendToStream(c *Chunk) {
	p.outStream <- c
}

func (p *ParallelReader) RowsRead() int {
	return int(p.Metrics.RowsRead)
}
