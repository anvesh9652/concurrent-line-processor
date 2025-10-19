package codes

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

func InitConvertCtoJ(r io.ReadCloser) {
	tf, err := os.Create("/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/test_conv.jsonl")
	clp.ExitOnError(err)
	defer tf.Close()

	clp.ExitOnError(ConvertCSVToJsonl(r, tf))
}

// The reader should have header column
func ConvertCSVToJsonl(r io.ReadCloser, w io.Writer) error {
	cols, rc := getColumns(r)
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

	nr := clp.NewConcurrentLineProcessor(io.NopCloser(rc),
		clp.WithChunkSize(chunkSize), clp.WithWorkers(workers), clp.WithRowsReadLimit(-1),
		clp.WithCustomLineProcessor(customProcessor),
	)

	if _, err := io.Copy(w, nr); err != nil {
		return err
	}
	// clp.PrintAsJsonString(nr.Metrics())
	return nil
}

func getColumns(r io.Reader) ([]string, io.Reader) {
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
	return strings.Split(string(firstLine), ","), br
}
