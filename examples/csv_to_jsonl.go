package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"strings"

	clp "github.com/anvesh9652/concurrent-line-processor"
)

func ConvertCSVToJsonl(r io.Reader) {
	// read frist row to headers
	var firstLine []byte
	br := bufio.NewReader(r)
	for {
		line, isLine, err := br.ReadLine()
		firstLine = append(firstLine, line...)
		clp.ExitOnError(err)
		if !isLine {
			break
		}
	}

	cols := strings.Split(string(firstLine), ",")
	customProcessor := func(b []byte) ([]byte, error) {
		cr := csv.NewReader(bytes.NewBuffer(b))
		row, err := cr.Read()
		if err != nil {
			return nil, err
		}
		md := map[string]string{}
		for i, col := range cols {
			md[col] = row[i]
		}
		return json.Marshal(md)
	}

	nr := clp.NewConcurrentLineProcessor(br,
		clp.WithChunkSize(chunkSize), clp.WithWorkers(workers), clp.WithRowsReadLimit(-1),
		clp.WithCustomLineProcessor(customProcessor),
	)

	tf, err := os.Create("/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/data/test_conv.jsonl")
	clp.ExitOnError(err)
	defer tf.Close()

	_, err = io.Copy(tf, nr)
	clp.ExitOnError(err)
	clp.PrintAsJsonString(nr.Metrics())
}
