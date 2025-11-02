package codes

import (
	"fmt"
	"io"
	"log"
	"os"

	clp "github.com/anvesh9652/concurrent-line-processor"
)

func MultiReaders(files []string) {
	var x []io.ReadCloser

	files = []string{files[2]}

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}
		x = append(x, f)
	}

	line := func(b []byte, info *clp.LineDetails) ([]byte, error) {
		return b, nil
	}

	pr := clp.NewConcurrentLineProcessor(nil, clp.WithMultiReaders(x...), clp.WithCustomLineProcessor(line))
	defer pr.Close()

	_, err := io.Copy(io.Discard, pr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(pr.Summary())
}
