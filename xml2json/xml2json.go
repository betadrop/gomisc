package main

import (
	"encoding/xml"
	"fmt"
	"os"
	"strings"
)

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
	var stack[] string;
        type TokenType int
	const ( 
	   NONE TokenType = iota
	   START
	   CHARDATA
	   END
        )
	var lastToken TokenType = NONE
	for {
		token, err := decoder.Token()
		if token == nil {
			break
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "token: ", err)
			break
		}
		if start, ok := token.(xml.StartElement); ok {
			if lastToken == START {
			   for i := 0; i < len(stack); i++ {
			       fmt.Print("   ");
			   }
			   fmt.Println(stack[len(stack) - 1]);
			}
                        stack = append(stack, start.Name.Local)
			lastToken = START
		} 
		if _, ok := token.(xml.EndElement); ok {
		   	stack = stack[:len(stack)-1]
			lastToken = END
		}
		if charData, ok := token.(xml.CharData); ok {
		        str := string(charData)
                        trimmed := strings.Trim(str, " \n\t")
                        if len(trimmed) > 0 {
                                if len(stack) > 0 {
				   for i := 0; i < len(stack); i++ {
				       fmt.Print("   ");
				   }
			  	   fmt.Println(stack[len(stack) - 1] + ": " + str);
				}
				lastToken = CHARDATA
			}
		}
	}
}
