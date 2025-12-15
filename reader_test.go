package concurrentlineprocessor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewReader_ReadsAllLines(t *testing.T) {
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

func TestNewReader_CustomLineProcessor(t *testing.T) {
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

func TestNewReader_CustomProcessorReturnsNil(t *testing.T) {
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

func TestNewReader_EmptyInput(t *testing.T) {
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

func TestNewReader_RowsReadLimit(t *testing.T) {
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

func TestNewReader_ErrorInCustomProcessor(t *testing.T) {
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

func TestNewReader_LargeInput(t *testing.T) {
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

func TestNewReader_AlwaysNewlineAtEnd(t *testing.T) {
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

func TestNewReader_Concurrency(t *testing.T) {
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

func TestNewReader_SmallChunkSize_OrderNotGuaranteed(t *testing.T) {
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

func TestNewReader_MultipleReaders(t *testing.T) {
	r1 := newReadCloser("alpha\nbeta\n")
	r2 := newReadCloser("gamma\ndelta\n")
	pr := NewConcurrentLineProcessor(nil, WithMultiReaders(r1, r2))
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

func TestNewReader_MultipleReadersLargeInput(t *testing.T) {
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
	pr := NewConcurrentLineProcessor(nil, WithMultiReaders(readers...), WithWorkers(4))
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

func TestNewReader_WithContext(t *testing.T) {
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
