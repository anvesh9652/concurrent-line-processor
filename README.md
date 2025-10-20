# Concurrent Line Processor

[![Go Reference](https://pkg.go.dev/badge/github.com/anvesh9652/concurrent-line-processor.svg)](https://pkg.go.dev/github.com/anvesh9652/concurrent-line-processor)
[![Go Report Card](https://goreportcard.com/badge/github.com/anvesh9652/concurrent-line-processor)](https://goreportcard.com/report/github.com/anvesh9652/concurrent-line-processor)

A high-performance, concurrent line-by-line processor for large files and streams in Go. This package splits input into chunks and processes lines concurrently using goroutines. You can also stitch multiple `io.ReadCloser` sources together and treat them as a single logical stream without extra fan-in plumbing.

## Features

- **Concurrent Processing**: Process lines concurrently with a configurable number of worker goroutines
- **Memory Efficient**: Uses a `sync.Pool` and streaming; never loads entire file into memory
- **Customizable**: Supply a thread-safe custom line processor function
- **Metrics**: Built-in metrics (bytes read, bytes transformed, rows read/written, processing duration)
- **Standard Interface**: Implements `io.Reader` and has a `Close()` for resource cleanup
- **Flexible Configuration**: Configure chunk size, worker count, channel size, and row read limit
- **Multi-source Input**: Merge multiple `io.ReadCloser` inputs into one stream (ordering between sources is nondeterministic)
- **Backpressure Friendly**: Internal bounded channels help balance producer/consumer throughput

## Installation

```bash
go get github.com/anvesh9652/concurrent-line-processor@latest
```

## Quick Start

Below are common usage patterns. Each example is self-contained and can be copied into a file and run with `go run`.

### Quick Start

Below are common usage patterns. Each example is self-contained and can be copied into a file and run with `go run`.

> Note: Reading via `io.ReadAll` will accumulate all processed data in memory. Prefer `io.Copy` to a file or stream for very large inputs.

#### 1. Basic Usage (stream to stdout)

```go
package main

import (
    "fmt"
    "io"
    "os"

    clp "github.com/anvesh9652/concurrent-line-processor"
)

func main() {
    file, err := os.Open("large-file.txt")
    if err != nil {
        panic(err)
    }
    defer file.Close()

    processor := clp.NewConcurrentLineProcessor(file)
    defer processor.Close() // only needed when multiple readers were supplied or to flush pipe early

    // Stream the processed output directly
    if _, err := io.Copy(os.Stdout, processor); err != nil {
        panic(err)
    }

    metrics := processor.Metrics()
    fmt.Printf("Processed %d rows, %d bytes in %s\n", metrics.RowsRead, metrics.BytesRead, metrics.TimeTook)
}
```

#### 2. Merging Multiple Sources (nondeterministic interleaving)

```go
package main

import (
    "io"
    "os"

    clp "github.com/anvesh9652/concurrent-line-processor"
)

func main() {
    files := []string{"part-1.log", "part-2.log", "part-3.log"}
    var readers []io.ReadCloser
    for _, name := range files {
        f, err := os.Open(name)
        if err != nil {
            panic(err)
        }
        readers = append(readers, f)
    }

    processor := clp.NewConcurrentLineProcessor(nil,
        clp.WithMultiReaders(readers...),
        clp.WithWorkers(4),
    )
    defer processor.Close()

    _, err := io.Copy(os.Stdout, processor)
    if err != nil {
        panic(err)
    }
}
```

#### 3. Custom Line Processing

```go
package main

import (
    "bytes"
    "io"
    "os"

    clp "github.com/anvesh9652/concurrent-line-processor"
)

func main() {
    file, err := os.Open("data.csv")
    if err != nil {
        panic(err)
    }
    defer file.Close()

    upperCaseProcessor := func(line []byte) ([]byte, error) {
        return bytes.ToUpper(line), nil
    }

    processor := clp.NewConcurrentLineProcessor(file,
        clp.WithCustomLineProcessor(upperCaseProcessor),
        clp.WithWorkers(8),
        clp.WithChunkSize(1024*1024), // 1MB chunks
    )
    defer processor.Close()

    outFile, err := os.Create("output.csv")
    if err != nil { panic(err) }
    defer outFile.Close()
    if _, err := io.Copy(outFile, processor); err != nil { panic(err) }
}
```

#### 4. CSV to JSONL Conversion

```go
package main

import (
    "bytes"
    "encoding/csv"
    "encoding/json"
    "io"
    "os"

    clp "github.com/anvesh9652/concurrent-line-processor"
)

func convertCSVToJSONL(inputFile, outputFile string, headers []string) error {
    input, err := os.Open(inputFile)
    if err != nil {
        return err
    }
    defer input.Close()

    output, err := os.Create(outputFile)
    if err != nil {
        return err
    }
    defer output.Close()

    csvToJSONProcessor := func(line []byte) ([]byte, error) {
        reader := csv.NewReader(bytes.NewReader(line))
        row, err := reader.Read()
        if err != nil {
            return nil, err
        }
        record := make(map[string]string)
        for i, header := range headers {
            if i < len(row) {
                record[header] = row[i]
            }
        }
        return json.Marshal(record)
    }

    processor := clp.NewConcurrentLineProcessor(input,
        clp.WithCustomLineProcessor(csvToJSONProcessor),
        clp.WithWorkers(4),
        clp.WithRowsReadLimit(-1),
    )

    _, err = io.Copy(output, processor)
    return err
}
```

#### 5. Processing with Row Limit

```go
package main

import (
    "fmt"
    "io"
    "os"

    clp "github.com/anvesh9652/concurrent-line-processor"
)

func processFirstThousandRows(filename string) error {
    file, err := os.Open(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    processor := clp.NewConcurrentLineProcessor(file,
        clp.WithRowsReadLimit(1000),
        clp.WithWorkers(2),
    )

    _, err = io.Copy(io.Discard, processor)
    if err != nil {
        return err
    }

    metrics := processor.Metrics()
    fmt.Printf("Processed %d rows in %s\n", metrics.RowsRead, metrics.TimeTook)
    return nil
}
```
Processing stops when the limit of newline-delimited rows is reached. Internally the reader may consume extra bytes to detect the boundary.

## Performance Considerations

### Memory Usage
- A `sync.Pool` minimizes per-chunk allocations.
- Memory scales with (chunk size * active workers) plus channel buffering.
- Default chunk size is 64KB (not 30KB as previously documented). Tune based on typical line length.

### Worker Count
- More workers help if your custom line processor is CPU-bound.
- For pure pass-through or I/O-bound workloads, increasing workers yields limited benefit.
- Default is `runtime.NumCPU()`.

### Chunk Size
- Larger chunks reduce syscall overhead but increase peak memory.
- Smaller chunks improve responsiveness and reduce memory footprint.
- Default: 64KB. Examples show tuning up to multi-MB for high-throughput transformations.

### Channel Size
- Controls buffering between reader, processors, and writer stages.
- Default: 70. Increase if workers frequently starve or if you have bursty input.

### Ordering
- Output line ordering relative to original input is preserved within individual chunks but overall ordering is **not guaranteed** when using multiple readers or very small chunk sizes with multiple workers.
- If strict ordering matters, run with `WithWorkers(1)`.

## Examples

The `examples/` directory contains complete examples demonstrating:

- Basic file processing
- CSV to JSONL conversion
- JSONL to CSV conversion
- Custom line transformations
- Multi-reader merging
- Performance profiling

## Metrics

The processor provides detailed metrics accessible via `Metrics()`:

```go
type Metrics struct {
    BytesRead        int64  `json:"bytes_read"`        // Total bytes read from source
    BytesTransformed int64  `json:"bytes_transformed"` // Total bytes after processing each line
    RowsRead         int64  `json:"rows_read"`         // Total rows processed
    RowsWritten      int64  `json:"rows_written"`      // Total rows written to the output stream
    TimeTook         string `json:"time_took"`         // Total processing time
}
```

## Thread Safety

- Call `Read` from a single goroutine at a time (standard `io.Reader` contract).
- The internal pipeline is concurrent; metrics fields are updated atomically and can be read safely at any time.
- Custom line processor functions must be thread-safe. Avoid mutating shared state unless you synchronize externally.
- When multiple source readers are configured, their data is consumed concurrently.

## Requirements

- Go 1.24.0 or later
- External dependency: `golang.org/x/sync` (errgroup)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## API Documentation

For complete API documentation, visit [pkg.go.dev](https://pkg.go.dev/github.com/anvesh9652/concurrent-line-processor).

## Error Handling

Errors encountered during any stage (reading, processing, writing) propagate through the internal pipe and surface on subsequent `Read` / `io.Copy`. After completion, `Metrics().TimeTook` reflects total duration even if an error occurred.

## Future Improvements

- Optional strict ordering mode using sequence numbers
- Pluggable backpressure / adaptive channel sizing
- Cancellation API (pass external context)
