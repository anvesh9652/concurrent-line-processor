package profiling

import (
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
)

var (
	cporfile = flag.String("cprofile", "cpuprofile", " ")
	mporfile = flag.String("mprofile", "memprofile", " ")
)

func WithProfiling(fn func()) {
	flag.Parse()
	cpuProfile()
	defer pprof.StopCPUProfile()
	fn()
	memProfile()
}

func cpuProfile() {
	if cporfile != nil {
		cf, _ := os.Create("./profiling/" + *cporfile + ".prof")
		if err := pprof.StartCPUProfile(cf); err != nil {
			log.Fatal(err)
		}
	}
}

func memProfile() {
	if mporfile != nil {
		mf, _ := os.Create("./profiling/" + *mporfile + ".prof")

		runtime.GC()
		if err := pprof.WriteHeapProfile(mf); err != nil {
			log.Fatal(err)
		}
		defer mf.Close()
	}
}
