package codes

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	clp "github.com/anvesh9652/concurrent-line-processor"
)

func InitConvertJtoC(file string) {
	f, err := os.Open(file)
	clp.ExitOnError(err)
	defer f.Close()

	cols, err := GetAllKeys(f, -1)
	clp.ExitOnError(err)

	f, err = os.Open(file)
	clp.ExitOnError(err)
	defer f.Close()

	tf, err := os.Create("/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/test_conv.csv")
	clp.ExitOnError(err)
	defer tf.Close()

	clp.ExitOnError(ConvertJsonlToCsv(cols, f, tf))
}

func GetAllKeys(r io.ReadCloser, rowsLimit int) ([]string, error) {
	keys := map[string]bool{}
	customProcessor := func(b []byte) ([]byte, error) {
		return processBytes(b, keys)
	}

	nr := clp.NewConcurrentLineProcessor(r,
		clp.WithChunkSize(chunkSize), clp.WithWorkers(workers), clp.WithRowsReadLimit(rowsLimit),
		clp.WithCustomLineProcessor(customProcessor),
	)
	if _, err := io.Copy(io.Discard, nr); err != nil {
		return nil, err
	}

	var columns []string
	for k := range keys {
		columns = append(columns, k)
	}
	// fmt.Println(nr.Summary())
	return columns, nil
}

// These functions can be reusalbe outside of this package
func ConvertJsonlToCsv(columns []string, r io.ReadCloser, w io.Writer) error {
	buffPool := sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 1024*3))
		},
	}
	customProcessor := func(b []byte) ([]byte, error) {
		var d map[string]any
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, nil
		}

		row := make([]string, len(columns))
		for i, col := range columns {
			row[i] = ConvertAnyToString(d[col])
		}

		buff := buffPool.Get().(*bytes.Buffer)
		buff.Reset()
		defer buffPool.Put(buff)

		cw := csv.NewWriter(buff)
		if err := cw.Write(row); err != nil {
			return nil, err
		}
		cw.Flush()
		return buff.Bytes(), nil
	}

	nr := clp.NewConcurrentLineProcessor(r,
		clp.WithChunkSize(chunkSize), clp.WithWorkers(workers), clp.WithRowsReadLimit(-1),
		clp.WithCustomLineProcessor(customProcessor),
	)

	if _, err := w.Write([]byte(strings.Join(columns, ",") + "\n")); err != nil {
		return err
	}

	_, err := io.Copy(w, nr)
	// fmt.Println(nr.Summary())
	return err
}

func processBytes(b []byte, keys map[string]bool) ([]byte, error) {
	var d map[string]any
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, err
	}

	mut.Lock()
	for k := range d {
		keys[k] = true
	}
	mut.Unlock()
	return b, nil
}

func ConvertAnyToString(v any) string {
	switch t := v.(type) {
	case int:
		return strconv.Itoa(t)
	case float64:
		return fmt.Sprintf("%f", t)
	case string:
		return t
	// Handle JSON arrays and objects by converting them to strings.
	case []any, map[string]any:
		bt, _ := json.Marshal(t)
		return string(bt)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%s", v)
	}
}
