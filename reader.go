package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"

	"golang.org/x/sync/errgroup"
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

		inStream:  make(chan *Chunk, 50),
		outStream: make(chan *Chunk, 50),

		customLineProcessor: func(b []byte) ([]byte, error) { return b, nil },

		pr: pr,
		pw: pw,
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

	if err := eg.Wait(); err != nil {
		fmt.Println("iam here:", err)
		p.pw.CloseWithError(err)
	}
}

func (p *ParallelReader) Read(b []byte) (int, error) {
	return p.pr.Read(b)
}

func (p *ParallelReader) readAsChunks() error {
	defer close(p.inStream)
	leftOver := make([]byte, 0, p.chunkSize*2)
	buff := make([]byte, p.chunkSize)
	chunkID := 1
	for {
		read, err := p.r.Read(buff)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(leftOver) != 0 {
					p.inStream <- &Chunk{id: chunkID, data: leftOver}
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

		p.inStream <- &Chunk{id: chunkID, data: copyToSend}
		chunkID++

	}
	return nil
}

func (p *ParallelReader) processChunks() error {
	defer close(p.outStream)
	poolErrG := errgroup.Group{}
	for range p.workers {
		poolErrG.Go(func() error {
			for chunk := range p.inStream {
				lineStart := 0
				for i, r := range chunk.data {
					if i == len(chunk.data)-1 {
						i++
					}
					if r == '\n' || i == len(chunk.data) {
						if err := p.processLineAndDispatch(chunk, lineStart, i); err != nil {
							return err
						}
						lineStart = i + 1
					}
				}
			}
			return nil
		})
	}
	return poolErrG.Wait()
}

func (p *ParallelReader) processLineAndDispatch(c *Chunk, s, e int) error {
	res, err := p.customLineProcessor(c.data)
	if err != nil {
		return err
	}
	c.data = WithNewLine(res)
	p.sendToStream(c)
	// atomic.AddInt64(&p.rowsRead, 1)
	return nil
}

func (p *ParallelReader) readProcessedData() error {
	for chunk := range p.outStream {
		// _ = data
		// printBytes(data)
		if _, err := p.pw.Write(chunk.data); err != nil {
			return err
		}
		p.rowsRead++
		if p.rowsReadLimit != -1 && p.rowsRead >= int64(p.rowsReadLimit) {
			break
		}
	}
	return p.pw.Close()
}

func (p *ParallelReader) sendToStream(c *Chunk) {
	p.outStream <- c
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
