package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

var (
	DefaultChunkSize = 1024 * 30 // 30 KB

	// DefaultWorkers is the number of goroutines to use for processing chunks
	DefaultWorkers = runtime.NumCPU()
)

// NewReader creates parallel reader
// use one worker to process it sequntially
func NewReader(r io.Reader, opts ...Option) *ParallelReader {
	pr, pw := io.Pipe()
	p := &ParallelReader{
		r:             r,
		workers:       DefaultWorkers,
		chunkSize:     DefaultChunkSize,
		rowsReadLimit: -1,

		inStream:  make(chan []byte, 50),
		outStream: make(chan []byte, 50),
		errChan:   make(chan error, 1),

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
		if err := p.readProcessedData(); err != nil {
			p.errChan <- err
		}
	}()

	// listen for errors
	go func() {
		for {
			fmt.Println("looping")
			// close all the channels properly
			if len(p.errChan) != 0 {
				fmt.Println("in error")
				close(p.inStream)
				close(p.outStream)
			}
		}
	}()
}

func (p *ParallelReader) Read(b []byte) (int, error) {
	if len(p.errChan) != 0 {
		err := <-p.errChan
		return 0, err
	}
	return p.pr.Read(b)
}

// Ignore this func
func (p *ParallelReader) ReadDataWithChunkOperation(readNumRows int) error {
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
			p.errChan <- err
			return // ?
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
					tempData := WithNewLine(data)
					atomic.AddInt64(&p.rowsRead, int64(bytes.Count(tempData, []byte("\n"))))
					p.sendToStream(tempData)
				} else {
					lineStart := 0
					for i, r := range data {
						if r == '\n' {
							err := p.processLineAndDispatch(data[lineStart:i])
							if err != nil {
								p.errChan <- err
							}
							lineStart = i + 1
						}
					}
					if lineStart != len(data) {
						err := p.processLineAndDispatch(data[lineStart:])
						if err != nil {
							p.errChan <- err
							return
						}
					}
				}
				if p.RowsRead() >= 7000 {
					p.errChan <- errors.New("this is test error")
					return
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

func (p *ParallelReader) RowsRead() int {
	return int(p.rowsRead)
}

func printBytes(data []byte) {
	fmt.Println(string(data))
}

func errWithDebugStack(err error) error {
	return errors.Join(err, fmt.Errorf("debug logs: %s", debug.Stack()))
}
