package main

import (
	"fmt"
	"time"
)

func goroutineCrash() {
	startTooMany()
}

var c int

func startTooMany() {
	fmt.Printf("RunTimeInfo: %+v\n", NewRunTimeInfo())
	for i := 0; i < 10E6; i++ {
		go start()
	}
	time.Sleep(1 * time.Second)
	fmt.Printf("count: %v\n", c)
}

func start() {
	//fmt.Printf("RunTimeInfo: %+v\n", NewRunTimeInfo())
}
