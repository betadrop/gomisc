package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"os"
)

func ReadFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintln(os.Stderr, "reading " + filename + ":", err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println("line: " + line)
		//fmt.Println("line: " + len(line) + " " + line)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
}

func Hash(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(file)
        h := sha1.New()
        _, err = io.Copy(h, reader)
	if err != nil {
		log.Fatal(err)
	}
	//log.Print(written)
        fmt.Printf("%x\n", h.Sum(nil))
}

func main() {
     //ReadFile("main.go")
     Hash("main.goj")
}
