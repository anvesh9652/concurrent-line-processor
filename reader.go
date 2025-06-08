package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	DefaultChunkSize = 1024 * 1024 // 1 MB

	// DefaultWorkers is the number of goroutines to use for reading
	DefaultWorkers = runtime.NumCPU()
)

type (
	Option        func(*ParallelReader)
	LineProcessor func([]byte) ([]byte, error)
)

type ParallelReader struct {
	r                   io.Reader
	chunkSize           int
	workers             int
	customLineProcessor LineProcessor

	rowsRead  int64
	inStream  chan []byte
	outStream chan []byte
}

// NewReader creates parallel reader
// use one worker to process it sequntially
func NewReader(r io.Reader, opts ...Option) *ParallelReader {
	p := &ParallelReader{
		r:         r,
		workers:   DefaultWorkers,
		chunkSize: DefaultChunkSize,
		inStream:  make(chan []byte, 100),
		outStream: make(chan []byte, 100),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
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

func (p *ParallelReader) Read(b []byte) (int, error) {
	return p.r.Read(b)
}

func (p *ParallelReader) ReadDataWithChunkOperation(readNumRows int) error {
	// this operation will be applied on each row
	go p.readAsChunks()
	go p.processChunks()
	p.readProcessedData()
	return nil
}

func (p *ParallelReader) readAsChunks() {
	leftOver := make([]byte, 0, p.chunkSize*2)
	buff := make([]byte, p.chunkSize)
	for {
		read, err := p.Read(buff)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(leftOver) != 0 {
					p.inStream <- leftOver
				}
				break
			}
			log.Fatal(err)
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

		p.inStream <- leftOver
	}
	close(p.inStream)
}

func (p *ParallelReader) processChunks() {
	wg := sync.WaitGroup{}
	for range p.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for data := range p.inStream {
				if p.customLineProcessor == nil {
					fmt.Println("here?")
					p.sendToStream(data)
					continue
				}

				var res []byte
				var err error
				start := 0
				for i, r := range data {
					if string(r) == "\n" {
						res, err = p.customLineProcessor(data[start:i])
						if err != nil {
							log.Fatal(err)
						}
						p.sendToStream(res)
						start = i + 1
					}
					atomic.AddInt64(&p.rowsRead, 1)
				}
				if start != len(data) {
					res, err = p.customLineProcessor(data[start:])
					if err != nil {
						log.Fatal(err)
					}
					p.sendToStream(res)
					atomic.AddInt64(&p.rowsRead, 1)
				}
			}
		}()
	}
	wg.Wait()
	close(p.outStream)
}

func (p *ParallelReader) readProcessedData() {
	for data := range p.outStream {
		_ = data
		// fmt.Println(string(data))
	}
}

func (p *ParallelReader) sendToStream(data []byte) {
	p.outStream <- data
}

func (p *ParallelReader) Close() {
	close(p.outStream)
}

func (p *ParallelReader) RowsRead() int {
	return int(p.rowsRead)
}
