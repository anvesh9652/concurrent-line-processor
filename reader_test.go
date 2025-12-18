package concurrentlineprocessor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConcurrentLineProcessor_ReadsAllLines(t *testing.T) {
	input := "line1\nline2\nline3\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r)
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.Equal(t, input, string(out))

	metrics := pr.Metrics()
	assert.Equal(t, int64(3), metrics.RowsRead)
	assert.Equal(t, int64(3), metrics.RowsWritten)
	assert.Equal(t, int64(len(input)), metrics.BytesRead)
	assert.Equal(t, int64(len(out)), metrics.BytesWritten)
	assert.Equal(t, 3, pr.RowsRead())
}

func TestConcurrentLineProcessor_CustomLineProcessor(t *testing.T) {
	input := "a\nb\nc\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(b []byte, _ *ChunkDetails, w io.Writer) error {
		_, err := w.Write(bytes.ToUpper(b))
		return err
	}))
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	expected := "A\nB\nC\n"
	assert.Equal(t, expected, string(out))

	metrics := pr.Metrics()
	assert.Equal(t, int64(3), metrics.RowsRead)
	assert.Equal(t, int64(3), metrics.RowsWritten)
	assert.Equal(t, int64(len(input)), metrics.BytesRead)
	assert.Equal(t, int64(len(out)), metrics.BytesWritten)
}

func TestConcurrentLineProcessor_LineProcessorSkipsOutput(t *testing.T) {
	const lines = 1500
	var sb strings.Builder
	for i := 0; i < lines; i++ {
		sb.WriteString("row")
		sb.WriteByte(':')
		sb.WriteString(strconv.Itoa(i))
		sb.WriteByte('\n')
	}
	r := newReadCloser(sb.String())

	// Custom processor that intentionally drops every line by
	pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(b []byte, _ *ChunkDetails, w io.Writer) error {
		return nil // valid case: skip output for this line
	}))

	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.Empty(t, out, "expected empty output when processor returns nil")

	// Validate metrics: rows read should equal input line count; rows/bytes written should be zero
	metrics := pr.Metrics()
	assert.Equal(t, int64(lines), metrics.RowsRead)
	assert.Equal(t, int64(0), metrics.RowsWritten)
	assert.Equal(t, int64(0), metrics.BytesWritten)
	assert.Equal(t, lines, pr.RowsRead())
}

func TestConcurrentLineProcessor_EmptyInput(t *testing.T) {
	r := newReadCloser("")
	pr := NewConcurrentLineProcessor(r)
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.Empty(t, out)

	metrics := pr.Metrics()
	assert.Equal(t, int64(0), metrics.RowsRead)
	assert.Equal(t, int64(0), metrics.RowsWritten)
	assert.Equal(t, int64(0), metrics.BytesRead)
	assert.Equal(t, int64(0), metrics.BytesWritten)
	assert.Equal(t, 0, pr.RowsRead())
}

func TestConcurrentLineProcessor_RowsReadLimit(t *testing.T) {
	input := "1\n2\n3\n4\n5\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r, WithRowsReadLimit(3))
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	expected := "1\n2\n3\n"
	assert.Equal(t, expected, string(out))

	metrics := pr.Metrics()
	assert.Equal(t, int64(3), metrics.RowsRead)
	assert.Equal(t, int64(3), metrics.RowsWritten)
	assert.Equal(t, int64(len(out)), metrics.BytesWritten)
	assert.Equal(t, 3, pr.RowsRead())
}

func TestConcurrentLineProcessor_ErrorInLineProcessor(t *testing.T) {
	input := "x\ny\nz\n"
	r := newReadCloser(input)
	errMsg := "fail on y"
	pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(b []byte, _ *ChunkDetails, w io.Writer) error {
		if string(b) == "y" {
			return errors.New(errMsg)
		}
		_, err := w.Write(b)
		return err
	}))
	_, err := io.ReadAll(pr)
	assert.Error(t, err)
	assert.Equal(t, errMsg, err.Error())
}

func TestConcurrentLineProcessor_LargeInput(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < 10000; i++ {
		sb.WriteString("row\n")
	}
	input := sb.String()
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r)
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.Equal(t, len(input), len(out))

	metrics := pr.Metrics()
	assert.Equal(t, int64(10000), metrics.RowsRead)
	assert.Equal(t, int64(10000), metrics.RowsWritten)
	assert.Equal(t, int64(len(input)), metrics.BytesRead)
	assert.Equal(t, int64(len(out)), metrics.BytesWritten)
	assert.Equal(t, 10000, pr.RowsRead())
}

func TestConcurrentLineProcessor_AlwaysNewlineAtEnd(t *testing.T) {
	input := "foo\nbar\nbaz"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r)
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.NotEmpty(t, out)
	assert.Equal(t, byte('\n'), out[len(out)-1], "expected output to have trailing newline")

	metrics := pr.Metrics()
	assert.Equal(t, int64(3), metrics.RowsRead)
	assert.Equal(t, int64(3), metrics.RowsWritten)
	assert.Greater(t, metrics.BytesWritten, int64(0))
}

func TestConcurrentLineProcessor_Concurrency(t *testing.T) {
	input := "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r, WithWorkers(4))
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.Equal(t, input, string(out))

	metrics := pr.Metrics()
	assert.Equal(t, int64(10), metrics.RowsRead)
	assert.Equal(t, int64(10), metrics.RowsWritten)
	assert.Equal(t, int64(len(input)), metrics.BytesRead)
	assert.Equal(t, int64(len(out)), metrics.BytesWritten)
	assert.Equal(t, 10, pr.RowsRead())
}

func TestConcurrentLineProcessor_SmallChunkSize_OrderNotGuaranteed(t *testing.T) {
	input := "a\nb\nc\nd\ne\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r, WithChunkSize(2)) // very small chunk size
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	// Split and compare as sets (ignoring order)
	inputLines := strings.Split(strings.TrimSpace(input), "\n")
	outputLines := strings.Split(strings.TrimSpace(string(out)), "\n")
	assert.Equal(t, len(inputLines), len(outputLines))

	lineCount := make(map[string]int)
	for _, l := range inputLines {
		lineCount[l]++
	}
	for _, l := range outputLines {
		lineCount[l]--
	}
	for l, c := range lineCount {
		assert.Equal(t, 0, c, "line %q count mismatch", l)
	}

	metrics := pr.Metrics()
	assert.Equal(t, int64(5), metrics.RowsRead)
	assert.Equal(t, int64(5), metrics.RowsWritten)
	assert.Equal(t, int64(len(input)), metrics.BytesRead)
	assert.Greater(t, metrics.BytesWritten, int64(0))
}

func TestConcurrentLineProcessor_MultipleReaders(t *testing.T) {
	r1 := newReadCloser("alpha\nbeta\n")
	r2 := newReadCloser("gamma\ndelta\n")
	pr := NewConcurrentLineProcessor(nil, WithReaders(r1, r2))
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)

	content := strings.TrimRight(string(out), "\n")
	assert.NotEmpty(t, content)

	lines := strings.Split(content, "\n")
	assert.Len(t, lines, 4)

	lineCount := map[string]int{
		"alpha": 1,
		"beta":  1,
		"gamma": 1,
		"delta": 1,
	}
	for _, l := range lines {
		lineCount[l]--
	}
	for l, c := range lineCount {
		assert.Equal(t, 0, c, "line %q count mismatch", l)
	}

	metrics := pr.Metrics()
	assert.Equal(t, int64(4), metrics.RowsRead)
	assert.Equal(t, int64(4), metrics.RowsWritten)
	assert.Greater(t, metrics.BytesRead, int64(0))
	assert.Greater(t, metrics.BytesWritten, int64(0))
	assert.Equal(t, 4, pr.RowsRead())
}

func TestConcurrentLineProcessor_MultipleReadersWithRowLimit(t *testing.T) {
	r1 := newReadCloser("line1\nline2\nline3\nline4\n")
	r2 := newReadCloser("line5\nline6\nline7\nline8\n")
	r3 := newReadCloser("line9\nline10\nline11\n")

	pr := NewConcurrentLineProcessor(nil, WithReaders(r1, r2, r3), WithRowsReadLimit(6))
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)

	content := strings.TrimRight(string(out), "\n")
	assert.NotEmpty(t, content)

	lines := strings.Split(content, "\n")
	assert.Len(t, lines, 6, "expected exactly 6 lines due to row limit")

	metrics := pr.Metrics()
	assert.Equal(t, int64(6), metrics.RowsRead)
	assert.Equal(t, int64(6), metrics.RowsWritten)
	assert.Equal(t, 6, pr.RowsRead())
}

func TestConcurrentLineProcessor_MultipleReadersLargeInput(t *testing.T) {
	const (
		readersCount   = 5
		linesPerReader = 20000
	)
	readers := make([]io.ReadCloser, 0, readersCount)
	expectedCounts := make(map[string]int, readersCount)
	for i := 0; i < readersCount; i++ {
		prefix := "reader" + strconv.Itoa(i)
		readers = append(readers, newReadCloser(buildReaderData(prefix, linesPerReader)))
		expectedCounts[prefix] = linesPerReader
	}
	pr := NewConcurrentLineProcessor(nil, WithReaders(readers...), WithWorkers(4))
	defer pr.Close()
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)

	lines := strings.Split(string(out), "\n")
	assert.NotEmpty(t, lines)

	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	assert.Equal(t, readersCount*linesPerReader, len(lines))

	for _, line := range lines {
		idx := strings.IndexByte(line, ':')
		assert.NotEqual(t, -1, idx, "unexpected line format: %q", line)
		prefix := line[:idx]
		expectedCounts[prefix]--
	}
	for prefix, remaining := range expectedCounts {
		assert.Equal(t, 0, remaining, "prefix %q count mismatch", prefix)
	}

	metrics := pr.Metrics()
	assert.Equal(t, int64(readersCount*linesPerReader), metrics.RowsRead)
	assert.Equal(t, int64(readersCount*linesPerReader), metrics.RowsWritten)
	assert.Greater(t, metrics.BytesRead, int64(0))
	assert.Greater(t, metrics.BytesWritten, int64(0))
	assert.Equal(t, readersCount*linesPerReader, pr.RowsRead())
}

func TestConcurrentLineProcessor_WithContext(t *testing.T) {
	t.Run("context timeout", func(t *testing.T) {
		// Create a large input that takes time to process
		const lines = 1000000
		var sb strings.Builder
		for i := 0; i < lines; i++ {
			sb.WriteString("row:")
			sb.WriteString(strconv.Itoa(i))
			sb.WriteByte('\n')
		}

		r := newReadCloser(sb.String())

		// Create a context with very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Nanosecond)
		defer cancel()

		pr := NewConcurrentLineProcessor(r, WithContext(ctx), WithWorkers(4))
		defer pr.Close()

		// Try to read all - should fail due to context timeout
		_, err := io.ReadAll(pr)
		assert.Error(t, err)
		if err != nil {
			assert.Contains(t, err.Error(), "context deadline exceeded")
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		// Create a large input
		const lines = 50000
		var sb strings.Builder
		for i := 0; i < lines; i++ {
			sb.WriteString("row:")
			sb.WriteString(strconv.Itoa(i))
			sb.WriteByte('\n')
		}

		r := newReadCloser(sb.String())

		// Create a cancellable context
		ctx, cancel := context.WithCancel(context.Background())

		pr := NewConcurrentLineProcessor(r, WithContext(ctx), WithWorkers(2))
		defer pr.Close()

		// Cancel immediately after starting
		cancel()

		// Try to read - should fail due to context cancellation
		_, err := io.ReadAll(pr)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})

	t.Run("context with sufficient timeout succeeds", func(t *testing.T) {
		// Test that context with sufficient timeout works fine
		input := "line1\nline2\nline3\n"
		r := newReadCloser(input)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		pr := NewConcurrentLineProcessor(r, WithContext(ctx))
		defer pr.Close()

		out, err := io.ReadAll(pr)
		assert.NoError(t, err)
		assert.Equal(t, input, string(out))

		metrics := pr.Metrics()
		assert.Equal(t, int64(3), metrics.RowsRead)
		assert.Equal(t, int64(3), metrics.RowsWritten)
	})
}

// Tests for WithCustomChunkProcessor

func TestConcurrentLineProcessor_CustomChunkProcessor(t *testing.T) {
	input := "line1\nline2\nline3\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r, WithCustomChunkProcessor(func(chunk []byte, _ *ChunkDetails, w io.Writer) error {
		// Transform entire chunk to uppercase
		_, err := w.Write(bytes.ToUpper(chunk))
		return err
	}))
	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	expected := "LINE1\nLINE2\nLINE3\n"
	assert.Equal(t, expected, string(out))

	metrics := pr.Metrics()
	assert.Equal(t, int64(3), metrics.RowsRead)
	assert.Greater(t, metrics.BytesWritten, int64(0))
}

func TestConcurrentLineProcessor_ChunkProcessorAggregation(t *testing.T) {
	// Test chunk processor that aggregates data within each chunk
	input := "1\n2\n3\n4\n5\n"
	r := newReadCloser(input)

	pr := NewConcurrentLineProcessor(r, WithCustomChunkProcessor(func(chunk []byte, info *ChunkDetails, w io.Writer) error {
		// Count lines in this chunk and write count
		lineCount := bytes.Count(chunk, []byte("\n"))
		_, err := w.Write([]byte(strconv.Itoa(lineCount)))
		return err
	}), WithWorkers(1)) // Single worker to ensure predictable output

	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.NotEmpty(t, out)
	// Output should contain the count (may have trailing newline from EnsureNewLineAtEnd)
	assert.Contains(t, string(out), "5")
}

func TestConcurrentLineProcessor_ChunkProcessorSkipsOutput(t *testing.T) {
	input := "data1\ndata2\ndata3\n"
	r := newReadCloser(input)

	pr := NewConcurrentLineProcessor(r, WithCustomChunkProcessor(func(chunk []byte, _ *ChunkDetails, w io.Writer) error {
		return nil // intentionally skip output
	}))

	out, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.Empty(t, out, "expected empty output when chunk processor writes nothing")

	metrics := pr.Metrics()
	assert.Equal(t, int64(3), metrics.RowsRead)
	assert.Equal(t, int64(0), metrics.RowsWritten)
}

func TestConcurrentLineProcessor_ChunkProcessorError(t *testing.T) {
	input := "line1\nline2\nline3\n"
	r := newReadCloser(input)
	expectedErr := "chunk processing failed"

	pr := NewConcurrentLineProcessor(r, WithCustomChunkProcessor(func(chunk []byte, _ *ChunkDetails, w io.Writer) error {
		return errors.New(expectedErr)
	}))

	_, err := io.ReadAll(pr)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err.Error())
}

// Tests for ChunkDetails

func TestConcurrentLineProcessor_ChunkDetailsInLineProcessor(t *testing.T) {
	input := "a\nb\nc\n"
	r := newReadCloser(input)

	var receivedDetails []ChunkDetails
	var mu sync.Mutex

	pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(line []byte, info *ChunkDetails, w io.Writer) error {
		mu.Lock()
		receivedDetails = append(receivedDetails, ChunkDetails{
			ReaderID: info.ReaderID,
			ChunkID:  info.ChunkID,
		})
		mu.Unlock()
		_, err := w.Write(line)
		return err
	}), WithWorkers(1)) // Single worker for predictable order

	_, err := io.ReadAll(pr)
	assert.NoError(t, err)

	// Should have received 3 ChunkDetails (one per line)
	assert.Len(t, receivedDetails, 3)

	// All should have ChunkID 0 for single chunk, ReaderID 0 for first reader
	for _, details := range receivedDetails {
		assert.Equal(t, 0, details.ReaderID)
		assert.GreaterOrEqual(t, details.ChunkID, 0)
	}
}

func TestConcurrentLineProcessor_ChunkDetailsInChunkProcessor(t *testing.T) {
	input := "chunk data here\n"
	r := newReadCloser(input)

	var receivedChunkID int
	var receivedReaderID int

	pr := NewConcurrentLineProcessor(r, WithCustomChunkProcessor(func(chunk []byte, info *ChunkDetails, w io.Writer) error {
		receivedChunkID = info.ChunkID
		receivedReaderID = info.ReaderID
		_, err := w.Write(chunk)
		return err
	}))

	_, err := io.ReadAll(pr)
	assert.NoError(t, err)

	assert.Equal(t, 0, receivedReaderID, "first reader should have ReaderID 0")
	assert.Equal(t, 0, receivedChunkID, "first chunk should have ChunkID 0")
}

func TestConcurrentLineProcessor_ChunkDetailsWithMultipleReaders(t *testing.T) {
	r1 := newReadCloser("reader0:line1\nreader0:line2\n")
	r2 := newReadCloser("reader1:line1\nreader1:line2\n")

	readerIDsSeen := make(map[int]bool)
	var mu sync.Mutex

	pr := NewConcurrentLineProcessor(nil,
		WithReaders(r1, r2),
		WithCustomLineProcessor(func(line []byte, info *ChunkDetails, w io.Writer) error {
			mu.Lock()
			readerIDsSeen[info.ReaderID] = true
			mu.Unlock()
			_, err := w.Write(line)
			return err
		}),
	)

	_, err := io.ReadAll(pr)
	assert.NoError(t, err)

	// Should have seen both reader IDs
	assert.True(t, readerIDsSeen[0] || readerIDsSeen[1], "should have seen at least one reader ID")
	assert.Len(t, readerIDsSeen, 2, "should have seen exactly 2 different reader IDs")
}

// Test Chunk.Write and Chunk.WriteByte methods

func TestChunk_Write(t *testing.T) {
	chunk := &Chunk{data: make([]byte, 10), endingPos: 0}

	n, err := chunk.Write([]byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, 5, chunk.endingPos)
	assert.Equal(t, "hello", string(chunk.data[:chunk.endingPos]))
}

func TestChunk_WriteOverflow(t *testing.T) {
	chunk := &Chunk{data: make([]byte, 5), endingPos: 0}

	// Write more than capacity
	n, err := chunk.Write([]byte("hello world"))
	assert.NoError(t, err)
	assert.Equal(t, 11, n)
	assert.Equal(t, 11, chunk.endingPos)
	assert.Equal(t, "hello world", string(chunk.data[:chunk.endingPos]))
}

func TestChunk_WriteByte(t *testing.T) {
	chunk := &Chunk{data: make([]byte, 10), endingPos: 0}

	err := chunk.WriteByte('X')
	assert.NoError(t, err)
	assert.Equal(t, 1, chunk.endingPos)
	assert.Equal(t, byte('X'), chunk.data[0])
}

func TestChunk_WriteByteOverflow(t *testing.T) {
	chunk := &Chunk{data: make([]byte, 1), endingPos: 1}

	// Writing when at capacity should append
	err := chunk.WriteByte('Y')
	assert.NoError(t, err)
	assert.Equal(t, 2, chunk.endingPos)
	assert.Equal(t, byte('Y'), chunk.data[1])
}

func newReadCloser(input string) io.ReadCloser {
	return io.NopCloser(strings.NewReader(input))
}

func buildReaderData(prefix string, lines int) string {
	var sb strings.Builder
	for i := 0; i < lines; i++ {
		sb.WriteString(prefix)
		sb.WriteByte(':')
		sb.WriteString(strconv.Itoa(i))
		sb.WriteByte('\n')
	}
	return sb.String()
}
