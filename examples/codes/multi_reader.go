package codes

import (
	"fmt"
	"io"
	"log"
	"os"
	"slices"

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

	pr := clp.NewConcurrentLineProcessor(nil, clp.WithMultiReaders(x...))
	defer pr.Close()

	f, err := os.Create("./tmp/multi_reader_output.jsonl")
	if err != nil {
		log.Fatal(err)
	}

	_, _ = io.Copy(f, pr)
	fmt.Println(pr.Summary())
}
