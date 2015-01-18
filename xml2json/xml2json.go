package main

import (
	"encoding/xml"
	"fmt"
	"os"
	"strings"
)

const (
	INDENT = "    "
)

func isStart(token xml.Token) bool {
	_, ok := token.(xml.StartElement)
	return ok
}

func startName(token xml.Token) string {
	if start, ok := token.(xml.StartElement); ok {
		return start.Name.Local
	}
	return ""
}

func isEnd(token xml.Token) bool {
	_, ok := token.(xml.EndElement)
	return ok
}

func endName(token xml.Token) string {
	if end, ok := token.(xml.EndElement); ok {
		return end.Name.Local
	}
	return ""
}

func isChar(token xml.Token) bool {
	_, ok := token.(xml.CharData)
	return ok
}

func getCharData(token xml.Token) string {
	if charData, ok := token.(xml.CharData); ok {
		return string(charData)
	}
	return ""
}

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Println("usage:", args[0], "file")
		os.Exit(1)
	}
	filename := args[1]
	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintln(os.Stderr, "reading "+filename+":", err)
	}
	decoder := xml.NewDecoder(file)
	var stack []string
	var lastElement string
	var lastToken xml.Token
	first := true
	for {
		var token xml.Token
		token, err := decoder.Token()
		if token == nil {
			break
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "token: ", err)
			break
		}
		// Skip empty character data
		if isChar(token) {
			str := string(getCharData(token))
			if len(strings.Trim(str, " \n\t")) == 0 {
				continue
			}
		}
		if isStart(token) {
			if isStart(lastToken) {
				if first {
					first = false
				} else {
					fmt.Println()
				}
				for i := 0; i < len(stack)-1; i++ {
					fmt.Print(INDENT)
				}
				fmt.Print(stack[len(stack)-1])
			}
			stack = append(stack, startName(token))
		}
		if isEnd(token) {
			lastElement = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
		}
		if isChar(token) {
			str := strings.Trim(getCharData(token), " \n\t")
			if len(stack) > 0 {
				// If the element is the same a last time then
				// display it like an array.
				currentElement := stack[len(stack)-1]
				if currentElement == lastElement {
					fmt.Print(", " + str)
				} else {
					fmt.Println()
					for i := 0; i < len(stack)-1; i++ {
						fmt.Print(INDENT)
					}
					fmt.Print(stack[len(stack)-1] + ": " + str)
				}
			}
		}
		lastToken = token
	}
	if !first {
		fmt.Println()
	}
}
