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
	var err error
	var x []io.ReadCloser

	// 1015862593 rows(1.01B) & 35GB worth of data
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			fmt.Println("err:", err)
			continue
		}
		x = append(x, f)
	}

	lp := func(b []byte, _ *clp.ChunkDetails, w io.Writer) error {
		_, err := w.Write(b)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	pr := clp.NewConcurrentLineProcessor(nil, clp.WithReaders(x...),
		clp.WithCustomLineProcessor(lp), clp.WithContext(ctx),
	)
	defer pr.Close()

	w := io.Discard
	// w := os.Stdout
	// w, err = os.Create("./tmp/multi_reader_output.txt")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	_, err = io.Copy(w, pr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(pr.Summary())
}

func directRead(rc []io.ReadCloser) {
	var r []io.Reader
	for _, i := range rc {
		r = append(r, i)
	}

	n, err := io.Copy(io.Discard, io.MultiReader(r...))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(clp.FormatBytes(float64(n), clp.BaseSI))
}
