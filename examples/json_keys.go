package main

import (
	"encoding/json"
	"io"

	clp "github.com/anvesh9652/concurrent-line-processor"
)

func GetAllKeys(r io.Reader) {
	keys := map[string]bool{}
	customProcessor := func(b []byte) ([]byte, error) {
		return processBytes(b, keys)
	}

	nr := clp.NewConcurrentLineProcessor(r,
		clp.WithChunkSize(chunkSize), clp.WithWorkers(workers), clp.WithRowsReadLimit(-1),
		clp.WithCustomLineProcessor(customProcessor),
	)
	_, err := io.Copy(io.Discard, nr)
	clp.ExitOnError(err)
	clp.PrintAsJsonString(keys)
	clp.PrintAsJsonString(nr.Metrics())
}

func processBytes(b []byte, keys map[string]bool) ([]byte, error) {
	var d map[string]any
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, err
	}

	d["test_key"] = "temp value"
	mut.Lock()
	for k := range d {
		keys[k] = true
	}
	mut.Unlock()
	return json.Marshal(d)
}
