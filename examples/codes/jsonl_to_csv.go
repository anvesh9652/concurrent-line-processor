package codes

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

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

	tf, err := os.Create("/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/test_conv.jsonl")
	clp.ExitOnError(err)
	defer tf.Close()

	clp.ExitOnError(ConvertJsonlToCsv(cols, f, tf))
}

// These functions can be reusalbe outside of this pacakge
func ConvertJsonlToCsv(columns []string, r io.Reader, w io.Writer) error {
	customProcessor := func(b []byte) ([]byte, error) {
		var d map[string]any
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, err
		}
		var row []string
		for _, col := range columns {
			row = append(row, ConvertAnyToString(d[col]))
		}
		return []byte(strings.Join(row, ",")), nil

	}

	nr := clp.NewConcurrentLineProcessor(r,
		clp.WithChunkSize(chunkSize), clp.WithWorkers(workers), clp.WithRowsReadLimit(-1),
		clp.WithCustomLineProcessor(customProcessor),
	)

	if _, err := w.Write([]byte(strings.Join(columns, ","))); err != nil {
		return err
	}

	_, err := io.Copy(w, nr)
	// clp.PrintAsJsonString(nr.Metrics())
	return err
}

func GetAllKeys(r io.Reader, rowsLimit int) ([]string, error) {
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
	// clp.PrintAsJsonString(nr.Metrics())
	return columns, nil
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
