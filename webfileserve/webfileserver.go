package main

import (
        "code.google.com/p/getopt"	
	"fmt"
	"log"
	"net/http"
	"os"
)

func serve(address string, root string) {
	log.Fatal(http.ListenAndServe(address,
		http.FileServer(http.Dir(root))))
}

func main() {
	args := os.Args
	if len(args) < 3 {
		fmt.Println("usage ", args[0], " host:port root_dir")
		os.Exit(1)
	}
	address := args[1]
	root := args[2]
	serve(address, root)
}
