package concurrentlineprocessor

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestNewReader_ReadsAllLines(t *testing.T) {
	input := "line1\nline2\nline3\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r)
	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != input {
		t.Errorf("expected %q, got %q", input, string(out))
	}
	if pr.RowsRead() != 3 {
		t.Errorf("expected 3 rows read, got %d", pr.RowsRead())
	}
}

func TestNewReader_CustomLineProcessor(t *testing.T) {
	input := "a\nb\nc\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(b []byte) ([]byte, error) {
		return bytes.ToUpper(b), nil
	}))
	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "A\nB\nC\n"
	if string(out) != expected {
		t.Errorf("expected %q, got %q", expected, string(out))
	}
}

func TestNewReader_EmptyInput(t *testing.T) {
	r := newReadCloser("")
	pr := NewConcurrentLineProcessor(r)
	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty output, got %q", string(out))
	}
	if pr.RowsRead() != 0 {
		t.Errorf("expected 0 rows read, got %d", pr.RowsRead())
	}
}

func TestNewReader_RowsReadLimit(t *testing.T) {
	input := "1\n2\n3\n4\n5\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r, WithRowsReadLimit(3))
	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "1\n2\n3\n"
	if string(out) != expected {
		t.Errorf("expected %q, got %q", expected, string(out))
	}
	if pr.RowsRead() != 3 {
		t.Errorf("expected 3 rows read, got %d", pr.RowsRead())
	}
}

func TestNewReader_ErrorInCustomProcessor(t *testing.T) {
	input := "x\ny\nz\n"
	r := newReadCloser(input)
	errMsg := "fail on y"
	pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(b []byte) ([]byte, error) {
		if string(b) == "y" {
			return nil, errors.New(errMsg)
		}
		return b, nil
	}))
	_, err := io.ReadAll(pr)
	if err == nil || err.Error() != errMsg {
		t.Errorf("expected error %q, got %v", errMsg, err)
	}
}

func TestNewReader_LargeInput(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < 10000; i++ {
		sb.WriteString("row\n")
	}
	r := newReadCloser(sb.String())
	pr := NewConcurrentLineProcessor(r)
	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != sb.Len() {
		t.Errorf("expected output len %d, got %d", sb.Len(), len(out))
	}
	if pr.RowsRead() != 10000 {
		t.Errorf("expected 10000 rows read, got %d", pr.RowsRead())
	}
}

func TestNewReader_AlwaysNewlineAtEnd(t *testing.T) {
	input := "foo\nbar\nbaz"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r)
	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) == 0 || out[len(out)-1] != '\n' {
		t.Errorf("expected output to have trailing newline, got %q", string(out))
	}
}

func TestNewReader_Concurrency(t *testing.T) {
	input := "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r, WithWorkers(4))
	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != input {
		t.Errorf("expected %q, got %q", input, string(out))
	}
	if pr.RowsRead() != 10 {
		t.Errorf("expected 10 rows read, got %d", pr.RowsRead())
	}
}

func TestNewReader_SmallChunkSize_OrderNotGuaranteed(t *testing.T) {
	input := "a\nb\nc\nd\ne\n"
	r := newReadCloser(input)
	pr := NewConcurrentLineProcessor(r, WithChunkSize(2)) // very small chunk size
	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Split and compare as sets (ignoring order)
	inputLines := strings.Split(strings.TrimSpace(input), "\n")
	outputLines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(inputLines) != len(outputLines) {
		t.Fatalf("expected %d lines, got %d", len(inputLines), len(outputLines))
	}
	lineCount := make(map[string]int)
	for _, l := range inputLines {
		lineCount[l]++
	}
	for _, l := range outputLines {
		lineCount[l]--
	}
	for l, c := range lineCount {
		if c != 0 {
			t.Errorf("line %q count mismatch: %d", l, c)
		}
	}
}

func TestNewReader_MultipleReaders(t *testing.T) {
	r1 := newReadCloser("alpha\nbeta\n")
	r2 := newReadCloser("gamma\ndelta\n")
	pr := NewConcurrentLineProcessor(nil, WithMultiReaders(r1, r2))
	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	content := strings.TrimRight(string(out), "\n")
	if len(content) == 0 {
		t.Fatalf("expected non-empty output, got %q", string(out))
	}
	lines := strings.Split(content, "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines, got %d", len(lines))
	}
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
		if c != 0 {
			t.Errorf("line %q count mismatch: %d", l, c)
		}
	}
	if pr.RowsRead() != 4 {
		t.Errorf("expected 4 rows read, got %d", pr.RowsRead())
	}
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lines := strings.Split(string(out), "\n")
	if len(lines) == 0 {
		t.Fatal("expected output, got empty slice")
	}
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) != readersCount*linesPerReader {
		t.Fatalf("expected %d lines, got %d", readersCount*linesPerReader, len(lines))
	}
	for _, line := range lines {
		idx := strings.IndexByte(line, ':')
		if idx == -1 {
			t.Fatalf("unexpected line format: %q", line)
		}
		prefix := line[:idx]
		expectedCounts[prefix]--
	}
	for prefix, remaining := range expectedCounts {
		if remaining != 0 {
			t.Errorf("prefix %q count mismatch: %d", prefix, remaining)
		}
	}
	if pr.RowsRead() != readersCount*linesPerReader {
		t.Errorf("expected %d rows read, got %d", readersCount*linesPerReader, pr.RowsRead())
	}
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
