package codes

import (
	"bytes"
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

	// tf, err := os.Create("/Users/agali/go-workspace/src/github.com/anvesh9652/concurrent-line-processor/tmp/test_conv.csv")
	// clp.ExitOnError(err)
	// defer tf.Close()

	clp.ExitOnError(ConvertJsonlToCsv(cols, f, io.Discard))
}

func GetAllKeys(r io.ReadCloser, rowsLimit int) ([]string, error) {
	var (
		mu   sync.Mutex
		keys = make(map[string]struct{})
	)
	customProcessor := func(b []byte, _ *clp.LineDetails) ([]byte, error) {
		var d map[string]any
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, err
		}
		mu.Lock()
		for k := range d {
			keys[k] = struct{}{}
		}
		mu.Unlock()
		return b, nil
	}

	nr := clp.NewConcurrentLineProcessor(r,
		clp.WithChunkSize(chunkSize), clp.WithWorkers(workers), clp.WithRowsReadLimit(rowsLimit),
		clp.WithCustomLineProcessor(customProcessor),
	)
	if _, err := io.Copy(io.Discard, nr); err != nil {
		return nil, err
	}

	columns := make([]string, 0, len(keys))
	for k := range keys {
		columns = append(columns, k)
	}
	// fmt.Println(nr.Summary())
	return columns, nil
}

// These functions can be reusalbe outside of this package
func ConvertJsonlToCsv(columns []string, r io.ReadCloser, w io.Writer) error {
	// pool of reusable buffers; keep them small initially, grow as needed
	buffPool := sync.Pool{
		New: func() any { return &bytes.Buffer{} },
	}

	// manual CSV escaping for a single field
	escapeField := func(field string, dst *bytes.Buffer) {
		// need quotes if field contains comma, quote, newline or leading/trailing space
		needsQuote := strings.ContainsAny(field, ",\n\r\"") || (len(field) > 0 && (field[0] == ' ' || field[len(field)-1] == ' '))
		if !needsQuote {
			dst.WriteString(field)
			return
		}
		dst.WriteByte('"')
		for i := 0; i < len(field); i++ {
			c := field[i]
			if c == '"' { // escape quotes by doubling
				dst.WriteByte('"')
			}
			dst.WriteByte(c)
		}
		dst.WriteByte('"')
	}

	customProcessor := func(b []byte, _ *clp.LineDetails) ([]byte, error) {
		var d map[string]any
		if err := json.Unmarshal(b, &d); err != nil {
			// skip malformed line silently
			return nil, nil
		}
		buff := buffPool.Get().(*bytes.Buffer)
		buff.Reset()
		// build CSV row manually
		for i, col := range columns {
			if i > 0 {
				buff.WriteByte(',')
			}
			escapeField(ConvertAnyToString(d[col]), buff)
		}
		buff.WriteByte('\n')
		out := append([]byte(nil), buff.Bytes()...) // copy to avoid data race when buff reused before consumer copies
		buffPool.Put(buff)
		return out, nil
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

func ConvertAnyToString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case int:
		return strconv.Itoa(t)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	case json.Number:
		return t.String()
	case []any, map[string]any:
		bt, _ := json.Marshal(t)
		return string(bt)
	default:
		// fallback using fmt for other types (e.g., structs)
		return fmt.Sprintf("%v", v)
	}
}
