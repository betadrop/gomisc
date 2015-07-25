package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("usage ", os.Args[0], " host:port root_dir")
		os.Exit(1)
	}
	address := os.Args[1]
	root := os.Args[2]
	if err := http.ListenAndServe(address, http.FileServer(http.Dir(root))); err != nil {
		log.Fatal(err)
	}
}
