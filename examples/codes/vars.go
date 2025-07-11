package codes

import (
	"sync"
)

var (
	workers   = 8
	chunkSize = 1024 * 1024 * 4 // 4 MB

	mut = sync.Mutex{}
)
