package concurrentlineprocessor

// import (
// 	"bytes"
// 	"errors"
// 	"io"
// 	"strings"
// 	"testing"
// )

// func TestNewReader_ReadsAllLines(t *testing.T) {
// 	input := "line1\nline2\nline3\n"
// 	r := strings.NewReader(input)
// 	pr := NewConcurrentLineProcessor(r)
// 	out, err := io.ReadAll(pr)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
// 	if string(out) != input {
// 		t.Errorf("expected %q, got %q", input, string(out))
// 	}
// 	if pr.RowsRead() != 3 {
// 		t.Errorf("expected 3 rows read, got %d", pr.RowsRead())
// 	}
// }

// func TestNewReader_CustomLineProcessor(t *testing.T) {
// 	input := "a\nb\nc\n"
// 	r := strings.NewReader(input)
// 	pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(b []byte) ([]byte, error) {
// 		return bytes.ToUpper(b), nil
// 	}))
// 	out, err := io.ReadAll(pr)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
// 	expected := "A\nB\nC\n"
// 	if string(out) != expected {
// 		t.Errorf("expected %q, got %q", expected, string(out))
// 	}
// }

// func TestNewReader_EmptyInput(t *testing.T) {
// 	r := strings.NewReader("")
// 	pr := NewConcurrentLineProcessor(r)
// 	out, err := io.ReadAll(pr)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
// 	if len(out) != 0 {
// 		t.Errorf("expected empty output, got %q", string(out))
// 	}
// 	if pr.RowsRead() != 0 {
// 		t.Errorf("expected 0 rows read, got %d", pr.RowsRead())
// 	}
// }

// func TestNewReader_RowsReadLimit(t *testing.T) {
// 	input := "1\n2\n3\n4\n5\n"
// 	r := strings.NewReader(input)
// 	pr := NewConcurrentLineProcessor(r, WithRowsReadLimit(3))
// 	out, err := io.ReadAll(pr)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
// 	expected := "1\n2\n3\n"
// 	if string(out) != expected {
// 		t.Errorf("expected %q, got %q", expected, string(out))
// 	}
// 	if pr.RowsRead() != 3 {
// 		t.Errorf("expected 3 rows read, got %d", pr.RowsRead())
// 	}
// }

// func TestNewReader_ErrorInCustomProcessor(t *testing.T) {
// 	input := "x\ny\nz\n"
// 	r := strings.NewReader(input)
// 	errMsg := "fail on y"
// 	pr := NewConcurrentLineProcessor(r, WithCustomLineProcessor(func(b []byte) ([]byte, error) {
// 		if string(b) == "y" {
// 			return nil, errors.New(errMsg)
// 		}
// 		return b, nil
// 	}))
// 	_, err := io.ReadAll(pr)
// 	if err == nil || err.Error() != errMsg {
// 		t.Errorf("expected error %q, got %v", errMsg, err)
// 	}
// }

// func TestNewReader_LargeInput(t *testing.T) {
// 	var sb strings.Builder
// 	for i := 0; i < 10000; i++ {
// 		sb.WriteString("row\n")
// 	}
// 	r := strings.NewReader(sb.String())
// 	pr := NewConcurrentLineProcessor(r)
// 	out, err := io.ReadAll(pr)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
// 	if len(out) != sb.Len() {
// 		t.Errorf("expected output len %d, got %d", sb.Len(), len(out))
// 	}
// 	if pr.RowsRead() != 10000 {
// 		t.Errorf("expected 10000 rows read, got %d", pr.RowsRead())
// 	}
// }

// func TestNewReader_AlwaysNewlineAtEnd(t *testing.T) {
// 	input := "foo\nbar\nbaz"
// 	r := strings.NewReader(input)
// 	pr := NewConcurrentLineProcessor(r)
// 	out, err := io.ReadAll(pr)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
// 	if string(out[len(out)-1]) != "\n" {
// 		t.Errorf("expected output to have trailing newline, got %q", string(out))
// 	}
// }

// func TestNewReader_Concurrency(t *testing.T) {
// 	input := "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"
// 	r := strings.NewReader(input)
// 	pr := NewConcurrentLineProcessor(r, WithWorkers(4))
// 	out, err := io.ReadAll(pr)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
// 	if string(out) != input {
// 		t.Errorf("expected %q, got %q", input, string(out))
// 	}
// 	if pr.RowsRead() != 10 {
// 		t.Errorf("expected 10 rows read, got %d", pr.RowsRead())
// 	}
// }

// func TestNewReader_SmallChunkSize_OrderNotGuaranteed(t *testing.T) {
// 	input := "a\nb\nc\nd\ne\n"
// 	r := strings.NewReader(input)
// 	pr := NewConcurrentLineProcessor(r, WithChunkSize(2)) // very small chunk size
// 	out, err := io.ReadAll(pr)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
// 	// Split and compare as sets (ignoring order)
// 	inputLines := strings.Split(strings.TrimSpace(input), "\n")
// 	outputLines := strings.Split(strings.TrimSpace(string(out)), "\n")
// 	if len(inputLines) != len(outputLines) {
// 		t.Fatalf("expected %d lines, got %d", len(inputLines), len(outputLines))
// 	}
// 	lineCount := make(map[string]int)
// 	for _, l := range inputLines {
// 		lineCount[l]++
// 	}
// 	for _, l := range outputLines {
// 		lineCount[l]--
// 	}
// 	for l, c := range lineCount {
// 		if c != 0 {
// 			t.Errorf("line %q count mismatch: %d", l, c)
// 		}
// 	}
// }
