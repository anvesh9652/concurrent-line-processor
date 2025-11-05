package concurrentlineprocessor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
			AppendNewLine(nil)
		})
	})

	t.Run("chunk with endingPos 0 does nothing", func(t *testing.T) {
		chunk := &Chunk{data: make([]byte, 10), endingPos: 0}
		AppendNewLine(chunk)
		assert.Equal(t, 0, chunk.endingPos)
	})

	t.Run("chunk without trailing newline adds one", func(t *testing.T) {
		chunk := &Chunk{data: []byte("hello     "), endingPos: 5}
		AppendNewLine(chunk)
		assert.Equal(t, 6, chunk.endingPos)
		assert.Equal(t, byte('\n'), chunk.data[5])
	})

	t.Run("chunk with trailing newline does nothing", func(t *testing.T) {
		chunk := &Chunk{data: []byte("hello\n    "), endingPos: 6}
		AppendNewLine(chunk)
		assert.Equal(t, 6, chunk.endingPos)
	})

	t.Run("chunk at capacity appends newline", func(t *testing.T) {
		chunk := &Chunk{data: []byte("hello"), endingPos: 5}
		AppendNewLine(chunk)
		assert.Equal(t, 6, chunk.endingPos)
		assert.Equal(t, 6, len(chunk.data))
		assert.Equal(t, byte('\n'), chunk.data[5])
	})
}

func TestFormatBytes(t *testing.T) {
	t.Run("bytes less than 1KB", func(t *testing.T) {
		result := FormatBytes(512)
		assert.Equal(t, "512B", result)
	})

	t.Run("exact 1KB", func(t *testing.T) {
		result := FormatBytes(1024)
		assert.Equal(t, "1KB", result)
	})

	t.Run("kilobytes with decimals", func(t *testing.T) {
		result := FormatBytes(1536) // 1.5KB
		assert.Equal(t, "1.5KB", result)
	})

	t.Run("exact 1MB", func(t *testing.T) {
		result := FormatBytes(1024 * 1024)
		assert.Equal(t, "1MB", result)
	})

	t.Run("megabytes with decimals", func(t *testing.T) {
		result := FormatBytes(2.5 * 1024 * 1024)
		assert.Equal(t, "2.5MB", result)
	})

	t.Run("exact 1GB", func(t *testing.T) {
		result := FormatBytes(1024 * 1024 * 1024)
		assert.Equal(t, "1GB", result)
	})

	t.Run("gigabytes with decimals", func(t *testing.T) {
		result := FormatBytes(3.75 * 1024 * 1024 * 1024)
		assert.Equal(t, "3.75GB", result)
	})

	t.Run("zero bytes", func(t *testing.T) {
		result := FormatBytes(0)
		assert.Equal(t, "0B", result)
	})

	t.Run("removes trailing zeros", func(t *testing.T) {
		result := FormatBytes(1024 * 10) // 10KB exactly
		assert.Equal(t, "10KB", result)
	})
}

func TestFormatDuration(t *testing.T) {
	t.Run("nanoseconds", func(t *testing.T) {
		result := FormatDuration(500 * time.Nanosecond)
		assert.Equal(t, "500ns", result)
	})

	t.Run("microseconds", func(t *testing.T) {
		result := FormatDuration(1500 * time.Microsecond)
		// Rounds to nearest microsecond
		assert.Equal(t, "2ms", result)
	})

	t.Run("milliseconds", func(t *testing.T) {
		result := FormatDuration(250 * time.Millisecond)
		assert.Equal(t, "250ms", result)
	})

	t.Run("seconds with rounding", func(t *testing.T) {
		result := FormatDuration(1234*time.Millisecond + 567*time.Microsecond)
		// Should round to 10ms precision
		assert.Equal(t, "1.23s", result)
	})

	t.Run("exact minute", func(t *testing.T) {
		result := FormatDuration(1 * time.Minute)
		assert.Equal(t, "1m0s", result)
	})

	t.Run("minutes with seconds", func(t *testing.T) {
		result := FormatDuration(2*time.Minute + 30*time.Second)
		assert.Equal(t, "2m30s", result)
	})

	t.Run("hours", func(t *testing.T) {
		result := FormatDuration(1*time.Hour + 15*time.Minute + 30*time.Second)
		assert.Equal(t, "1h15m30s", result)
	})

	t.Run("zero duration", func(t *testing.T) {
		result := FormatDuration(0)
		assert.Equal(t, "0s", result)
	})
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
