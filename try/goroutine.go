package main

import (
	"fmt"
	"runtime"
	"time"
)

func goroutine() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	startChannels()
}

type Token struct {
	Id    int
	Count int
	Limit int
	Inc   int64
}

var channels []chan Token
var end chan Token

func startChannels() {
	channels = make([]chan Token, 100)
	for i, _ := range channels {
		channels[i] = make(chan Token)
	}
	end = make(chan Token)
	for i, _ := range channels {
		go routine(i)
	}
	var st Token
	st.Limit = 800
	fmt.Printf("Start token: %+v\n", st)
	start := time.Now()
	for i := 0; i < 8; i++ {
		st.Id = i
		channels[0] <- st
		time.Sleep(1 * time.Second)
	}
	for i := 0; i < 8; i++ {
		et := <-end
		fmt.Printf("End token: %+v duration %v\n", et, time.Since(start))
	}
}

func routine(i int) {
	in := channels[i]
	out := channels[(i+1)%len(channels)]
	for {
		t := <-in
		fmt.Printf("%v Token: %+v\n", i, t)
		//fmt.Printf("RunTimeInfo: %+v\n", NewRunTimeInfo())
		t.Count++
		var i int64
		var l int64 = int64(t.Count * 1E5)
		for i = 0; i < l; i++ {
			t.Inc = t.Inc + i
		}
		t.Count++
		if t.Count >= t.Limit {
			end <- t
		} else {
			out <- t
		}
	}
}
