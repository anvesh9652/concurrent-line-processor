package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	DefaultChunkSize = 1024 * 24 // 1 MB

	// DefaultWorkers is the number of goroutines to use for reading
	DefaultWorkers = runtime.NumCPU()
)

// NewReader creates parallel reader
// use one worker to process it sequntially
func NewReader(r io.Reader, opts ...Option) *ParallelReader {
	pr, pw := io.Pipe()
	p := &ParallelReader{
		r:         r,
		workers:   DefaultWorkers,
		chunkSize: DefaultChunkSize,
		inStream:  make(chan []byte, 100),
		outStream: make(chan []byte, 100),

		pr: pr,
		pw: pw,
	}

	for _, opt := range opts {
		opt(p)
	}
	p.start()
	return p
}

func (p *ParallelReader) start() {
	go p.readAsChunks()
	go p.processChunks()
	go func() {
		err := p.readProcessedData()
		ExistOnError(err)
	}()
}

func (p *ParallelReader) Read(b []byte) (int, error) {
	return p.pr.Read(b)
}

// Ignore this func
func (p *ParallelReader) ReadDataWithChunkOperation(readNumRows int) error {
	// this operation will be applied on each row
	go p.readAsChunks()
	go p.processChunks()

	// go p.readProcessedData(pw)
	return nil
}

func (p *ParallelReader) readAsChunks() {
	leftOver := make([]byte, 0, p.chunkSize*2)
	buff := make([]byte, p.chunkSize)
	for {
		read, err := p.r.Read(buff)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(leftOver) != 0 {
					p.inStream <- leftOver
				}
				break
			}
			ExistOnError(err)
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

		p.inStream <- copyToSend
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
					p.sendToStream(WithNewLine(data))
					continue
				}

				lineStart := 0
				for i, r := range data {
					if string(r) == "\n" {
						ExistOnError(p.processLineAndDispatch(data[lineStart:i]))
						lineStart = i + 1
					}
				}
				if lineStart != len(data) {
					ExistOnError(p.processLineAndDispatch(data[lineStart:]))
				}
			}
		}()
	}
	wg.Wait()
	close(p.outStream)
}

func (p *ParallelReader) processLineAndDispatch(data []byte) error {
	res, err := p.customLineProcessor(data)
	if err != nil {
		return err
	}
	p.sendToStream(WithNewLine(res))
	atomic.AddInt64(&p.rowsRead, 1)
	return nil
}

func (p *ParallelReader) readProcessedData() error {
	for data := range p.outStream {
		// _ = data
		// printBytes(data)
		if _, err := p.pw.Write(data); err != nil {
			return err
		}
	}
	return p.pw.Close()
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

func printBytes(data []byte) {
	fmt.Println(string(data))
}
