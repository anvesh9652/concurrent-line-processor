package codes

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	clp "github.com/anvesh9652/concurrent-line-processor"
)

func MultiReaders(files []string) {
	var x []io.ReadCloser

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}
		x = append(x, f)
	}
	lp := func(b []byte, _ *clp.LineDetails) ([]byte, error) {
		buff := bytes.NewBuffer(b)
		// buff.Write(append([]byte("\n"), b...))
		return buff.Bytes(), nil
	}

	pr := clp.NewConcurrentLineProcessor(nil, clp.WithMultiReaders(x...), clp.WithCustomLineProcessor(lp))
	defer pr.Close()

	_, err := io.Copy(io.Discard, pr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(pr.Summary())
}
