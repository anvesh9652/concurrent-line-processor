# Concurrent Line Processor

[![Go Reference](https://pkg.go.dev/badge/github.com/anvesh9652/concurrent-line-processor.svg)](https://pkg.go.dev/github.com/anvesh9652/concurrent-line-processor)
[![Go Report Card](https://goreportcard.com/badge/github.com/anvesh9652/concurrent-line-processor)](https://goreportcard.com/report/github.com/anvesh9652/concurrent-line-processor)

A high-performance, concurrent line-by-line processor for large files and streams in Go. This package enables efficient processing of large files by splitting input into chunks and processing each line concurrently using multiple goroutines. You can also stitch multiple data sources together and treat them as a single stream.

## Features

- **Concurrent Processing**: Process lines concurrently using configurable number of worker goroutines
- **Memory Efficient**: Uses memory pooling and streaming to handle large files without loading everything into memory
- **Customizable**: Support for custom line processing functions
- **Metrics**: Built-in performance metrics (bytes read, rows processed, processing time etc..)
- **Standard Interface**: Implements `io.ReadCloser` for seamless integration with existing Go I/O patterns
- **Flexible Configuration**: Configurable chunk size, worker count, and row limits
- **Multi-source Input**: Combine multiple `io.ReadCloser` inputs into a single logical stream without manual fan-in code

## Installation

```bash
go get github.com/anvesh9652/concurrent-line-processor@latest
```

## Quick Start

Below are common usage patterns. Each example is self-contained and can be copied into a file and run with `go run`.

#### 1. Basic Usage

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
    output, err := io.ReadAll(processor)
    if err != nil {
        panic(err)
    }

    fmt.Println(string(output))

    metrics := processor.Metrics()
    fmt.Printf("Processed %d rows, %d bytes in %s\n", metrics.RowsRead, metrics.BytesRead, metrics.TimeTook)
}
```

#### 2. Merging Multiple Sources

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

    output, err := io.ReadAll(processor)
    if err != nil {
        panic(err)
    }

    err = os.WriteFile("output.csv", output, 0644)
    if err != nil {
        panic(err)
    }
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
- Processing stops when the limit is reached

## Performance Considerations

### Memory Usage
- The processor uses memory pooling to minimize allocations
- Memory usage scales with chunk size and number of workers
- For very large files, consider smaller chunk sizes

### Worker Count
- More workers help with CPU-intensive line processing
- For I/O-bound operations, more workers may not help
- Start with `runtime.NumCPU()` and adjust based on your use case

### Chunk Size
- Larger chunks reduce overhead but increase memory usage
- Smaller chunks are more memory-efficient but may have higher overhead
- The default 30KB works well for most use cases

## Examples

The `examples/` directory contains complete examples demonstrating:

- Basic file processing
- CSV to JSONL conversion
- JSONL to CSV conversion
- Custom line transformations
- Performance profiling

## Metrics

The processor provides detailed metrics accessible via the `Metrics()` method:

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

- The `concurrentLineProcessor` itself is safe for concurrent use
- Custom line processor functions must be thread-safe
- Metrics can be safely accessed concurrently
- The processor implements `io.Reader` and can be used safely by one goroutine at a time

## Requirements

- Go 1.24.0 or later
- No external dependencies beyond `golang.org/x/sync`

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## API Documentation

For complete API documentation, visit [pkg.go.dev](https://pkg.go.dev/github.com/anvesh9652/concurrent-line-processor).
