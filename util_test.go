package concurrentlineprocessor

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLines(t *testing.T) {
	t.Run("empty input returns no lines", func(t *testing.T) {
		var result [][]byte
		for line := range Lines([]byte{}, false) {
			result = append(result, line)
		}
		assert.Empty(t, result)
	})

	t.Run("early break stops iteration", func(t *testing.T) {
		var result [][]byte
		for line := range Lines([]byte("line1\nline2\nline3\nline4"), false) {
			result = append(result, slices.Clone(line))
			if len(result) == 2 {
				break
			}
		}
		assert.Len(t, result, 2)
		assert.Equal(t, []byte("line1"), result[0])
		assert.Equal(t, []byte("line2"), result[1])
	})

	t.Run("rawLine=false strips newlines", func(t *testing.T) {
		t.Run("single line without newline", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("hello"), false) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 1)
			assert.Equal(t, []byte("hello"), result[0])
		})

		t.Run("single line with newline stripped", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("hello\n"), false) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 1)
			assert.Equal(t, []byte("hello"), result[0])
		})

		t.Run("multiple lines without trailing newline", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("line1\nline2\nline3"), false) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 3)
			assert.Equal(t, []byte("line1"), result[0])
			assert.Equal(t, []byte("line2"), result[1])
			assert.Equal(t, []byte("line3"), result[2])
		})

		t.Run("multiple lines with trailing newline", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("line1\nline2\nline3\n"), false) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 3)
			assert.Equal(t, []byte("line1"), result[0])
			assert.Equal(t, []byte("line2"), result[1])
			assert.Equal(t, []byte("line3"), result[2])
		})

		t.Run("empty lines are preserved", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("line1\n\nline3"), false) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 3)
			assert.Equal(t, []byte("line1"), result[0])
			assert.Equal(t, []byte(""), result[1])
			assert.Equal(t, []byte("line3"), result[2])
		})

		t.Run("only newlines", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("\n\n\n"), false) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 3)
			assert.Equal(t, []byte(""), result[0])
			assert.Equal(t, []byte(""), result[1])
			assert.Equal(t, []byte(""), result[2])
		})

		t.Run("single newline character", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("\n"), false) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 1)
			assert.Equal(t, []byte(""), result[0])
		})
	})

	t.Run("rawLine=true preserves newlines", func(t *testing.T) {
		t.Run("single line with newline preserved", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("hello\n"), true) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 1)
			assert.Equal(t, []byte("hello\n"), result[0])
		})

		t.Run("single line without trailing newline", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("hello"), true) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 1)
			assert.Equal(t, []byte("hello"), result[0])
		})

		t.Run("multiple lines with trailing newline", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("line1\nline2\nline3\n"), true) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 3)
			assert.Equal(t, []byte("line1\n"), result[0])
			assert.Equal(t, []byte("line2\n"), result[1])
			assert.Equal(t, []byte("line3\n"), result[2])
		})

		t.Run("multiple lines without trailing newline", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("line1\nline2\nline3"), true) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 3)
			assert.Equal(t, []byte("line1\n"), result[0])
			assert.Equal(t, []byte("line2\n"), result[1])
			assert.Equal(t, []byte("line3"), result[2])
		})

		t.Run("empty lines are preserved with newlines", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("line1\n\nline3\n"), true) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 3)
			assert.Equal(t, []byte("line1\n"), result[0])
			assert.Equal(t, []byte("\n"), result[1])
			assert.Equal(t, []byte("line3\n"), result[2])
		})

		t.Run("only newlines", func(t *testing.T) {
			var result [][]byte
			for line := range Lines([]byte("\n\n\n"), true) {
				result = append(result, slices.Clone(line))
			}
			assert.Len(t, result, 3)
			assert.Equal(t, []byte("\n"), result[0])
			assert.Equal(t, []byte("\n"), result[1])
			assert.Equal(t, []byte("\n"), result[2])
		})
	})
}

func TestIfNull(t *testing.T) {
	t.Run("with nil pointer returns default", func(t *testing.T) {
		var nilPtr *int
		result := IfNull(nilPtr, 42)
		assert.Equal(t, 42, result)
	})

	t.Run("with non-nil pointer returns pointer value", func(t *testing.T) {
		value := 100
		result := IfNull(&value, 42)
		assert.Equal(t, 100, result)
	})

	t.Run("with nil string pointer returns default", func(t *testing.T) {
		var nilStr *string
		result := IfNull(nilStr, "default")
		assert.Equal(t, "default", result)
	})

	t.Run("with non-nil string pointer returns pointer value", func(t *testing.T) {
		str := "hello"
		result := IfNull(&str, "default")
		assert.Equal(t, "hello", result)
	})

	t.Run("with zero value pointer returns zero value", func(t *testing.T) {
		zero := 0
		result := IfNull(&zero, 42)
		assert.Equal(t, 0, result)
	})
}

func TestAppendNewLine(t *testing.T) {
	t.Run("nil chunk does nothing", func(t *testing.T) {
		// Should not panic
		assert.NotPanics(t, func() {
			EnsureNewLineAtEnd(nil)
		})
	})

	t.Run("chunk with endingPos 0 does nothing", func(t *testing.T) {
		chunk := &Chunk{data: make([]byte, 10), endingPos: 0}
		EnsureNewLineAtEnd(chunk)
		assert.Equal(t, 0, chunk.endingPos)
	})

	t.Run("chunk without trailing newline adds one", func(t *testing.T) {
		chunk := &Chunk{data: []byte("hello     "), endingPos: 5}
		EnsureNewLineAtEnd(chunk)
		assert.Equal(t, 6, chunk.endingPos)
		assert.Equal(t, byte('\n'), chunk.data[5])
	})

	t.Run("chunk with trailing newline does nothing", func(t *testing.T) {
		chunk := &Chunk{data: []byte("hello\n    "), endingPos: 6}
		EnsureNewLineAtEnd(chunk)
		assert.Equal(t, 6, chunk.endingPos)
	})

	t.Run("chunk at capacity appends newline", func(t *testing.T) {
		chunk := &Chunk{data: []byte("hello"), endingPos: 5}
		EnsureNewLineAtEnd(chunk)
		assert.Equal(t, 6, chunk.endingPos)
		assert.Equal(t, 6, len(chunk.data))
		assert.Equal(t, byte('\n'), chunk.data[5])
	})
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name  string
		bytes float64
		base  float64
		want  string
	}{
		// SI Base (1000) Tests
		{name: "SI: bytes less than base", bytes: 512, base: BaseSI, want: "512B"},
		{name: "SI: exact 1KB", bytes: 1000, base: BaseSI, want: "1KB"},
		{name: "SI: kilobytes with decimals", bytes: 1500, base: BaseSI, want: "1.5KB"},
		{name: "SI: exact 1MB", bytes: 1000 * 1000, base: BaseSI, want: "1MB"},
		{name: "SI: megabytes with decimals", bytes: 2.5 * 1000 * 1000, base: BaseSI, want: "2.5MB"},
		{name: "SI: exact 1GB", bytes: 1000 * 1000 * 1000, base: BaseSI, want: "1GB"},
		{name: "SI: gigabytes with decimals", bytes: 3.75 * 1000 * 1000 * 1000, base: BaseSI, want: "3.75GB"},
		{name: "SI: removes trailing zeros", bytes: 10000, base: BaseSI, want: "10KB"},

		// Binary Base (1024) Tests
		{name: "Binary: bytes less than base", bytes: 512, base: BaseBinary, want: "512B"},
		{name: "Binary: exact 1KiB", bytes: 1024, base: BaseBinary, want: "1KiB"},
		{name: "Binary: kibibytes with decimals", bytes: 1536, base: BaseBinary, want: "1.5KiB"},
		{name: "Binary: exact 1MiB", bytes: 1024 * 1024, base: BaseBinary, want: "1MiB"},
		{name: "Binary: mebibytes with decimals", bytes: 2.5 * 1024 * 1024, base: BaseBinary, want: "2.5MiB"},
		{name: "Binary: exact 1GiB", bytes: 1024 * 1024 * 1024, base: BaseBinary, want: "1GiB"},
		{name: "Binary: gibibytes with decimals", bytes: 3.75 * 1024 * 1024 * 1024, base: BaseBinary, want: "3.75GiB"},
		{name: "Binary: removes trailing zeros", bytes: 10 * 1024, base: BaseBinary, want: "10KiB"},

		// Edge Cases
		{name: "zero bytes SI", bytes: 0, base: BaseSI, want: "0B"},
		{name: "zero bytes Binary", bytes: 0, base: BaseBinary, want: "0B"},
		{name: "very small decimal values", bytes: 1024.01, base: BaseBinary, want: "1KiB"},
		{name: "fractional KB/KiB", bytes: 2048.5, base: BaseBinary, want: "2KiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatBytes(tt.bytes, tt.base)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{name: "nanoseconds", duration: 500 * time.Nanosecond, want: "500ns"},
		{name: "microseconds", duration: 1500 * time.Microsecond, want: "2ms"},
		{name: "milliseconds", duration: 250 * time.Millisecond, want: "250ms"},
		{name: "seconds with rounding", duration: 1234*time.Millisecond + 567*time.Microsecond, want: "1.23s"},
		{name: "exact minute", duration: 1 * time.Minute, want: "1m0s"},
		{name: "minutes with seconds", duration: 2*time.Minute + 30*time.Second, want: "2m30s"},
		{name: "hours", duration: 1*time.Hour + 15*time.Minute + 30*time.Second, want: "1h15m30s"},
		{name: "zero duration", duration: 0, want: "0s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatDuration(tt.duration)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestFilter(t *testing.T) {
	t.Run("empty slice returns empty slice", func(t *testing.T) {
		var empty []int
		result := Filter(empty, func(i int) bool { return i > 0 })
		assert.Empty(t, result)
	})

	t.Run("filter integers greater than 5", func(t *testing.T) {
		nums := []int{1, 6, 3, 8, 2, 9}
		result := Filter(nums, func(i int) bool { return i > 5 })
		expected := []int{6, 8, 9}
		assert.Equal(t, expected, result)
	})

	t.Run("filter strings by length", func(t *testing.T) {
		words := []string{"a", "hello", "go", "world", "x"}
		result := Filter(words, func(s string) bool { return len(s) > 2 })
		expected := []string{"hello", "world"}
		assert.Equal(t, expected, result)
	})

	t.Run("no elements match filter", func(t *testing.T) {
		nums := []int{1, 2, 3}
		result := Filter(nums, func(i int) bool { return i > 10 })
		assert.Empty(t, result)
	})

	t.Run("all elements match filter", func(t *testing.T) {
		nums := []int{2, 4, 6, 8}
		result := Filter(nums, func(i int) bool { return i%2 == 0 })
		assert.Equal(t, nums, result)
	})

	t.Run("filter custom struct", func(t *testing.T) {
		type person struct {
			name string
			age  int
		}
		people := []person{
			{"Alice", 25},
			{"Bob", 17},
			{"Charlie", 30},
			{"Diana", 16},
		}
		result := Filter(people, func(p person) bool { return p.age >= 18 })
		assert.Len(t, result, 2)
		assert.Equal(t, "Alice", result[0].name)
		assert.Equal(t, "Charlie", result[1].name)
	})
}
