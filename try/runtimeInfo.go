package main

import (
	"fmt"
	"runtime"
)

type RunTimeInfo struct {
	NumCPU       int
	GOMAXPROCS   int
	NumGoroutine int
	NumCgoCall   int64
}

func NewRunTimeInfo() (info *RunTimeInfo) {
	info = new(RunTimeInfo)
	info.get()
	return info
}

func (info *RunTimeInfo) get() {
	info.NumCPU = runtime.NumCPU()
	info.GOMAXPROCS = runtime.GOMAXPROCS(0)
	info.NumGoroutine = runtime.NumGoroutine()
	info.NumCgoCall = runtime.NumCgoCall()
}

func runtimeInfo() {
	info := NewRunTimeInfo()
	fmt.Printf("RunTimeInfo: %+v\n", info)
}
