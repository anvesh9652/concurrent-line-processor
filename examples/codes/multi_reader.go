package codes

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	clp "github.com/anvesh9652/concurrent-line-processor"
)

func MultiReaders(files []string) {
	var x []io.ReadCloser

	// 1015862593 rows(1.01B) & 35GB worth of data
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}
		x = append(x, f)
	}
	lp := func(b []byte, _ *clp.LineDetails) ([]byte, error) {
		return b, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pr := clp.NewConcurrentLineProcessor(nil, clp.WithMultiReaders(x...), clp.WithCustomLineProcessor(lp), clp.WithContext(ctx))
	defer pr.Close()

	_, err := io.Copy(io.Discard, pr)
	if err != nil {
		log.Fatal(err)
	}
	// chunkSize=64KB workers=10 bytesRead=34.99GB bytesWritten=34.99GB rowsRead=1015862593 rowsWritten=1015862594 throughput=5.74GB/s elapsed=6.09s
	fmt.Println(pr.Summary())
}
