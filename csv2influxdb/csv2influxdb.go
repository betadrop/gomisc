package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
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
	reader := csv.NewReader(bufio.NewReader(file))
	var lineNum int
	for {
		records, err := reader.Read()
		lineNum = lineNum + 1
		if records != nil {
			update, err := ReadUpdate(records)
			if err == nil {
				treatUpdate(update)
			} else {
				fmt.Fprintln(os.Stderr, "line", lineNum, "error:", err)
				os.Exit(1)
			}
		}
		if err != nil {
			if err != io.EOF {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			break
		}
	}
}

const (
	stampFormat = "HH:MM:SS.XXXXXX"
)

type PriceSize struct {
	Price float64
	Size  int64
}

type MarketUpdate struct {
	Timestamp time.Time
	Bid       PriceSize
	Ask       PriceSize
	Last      PriceSize
}

func ReadUpdate(records []string) (*MarketUpdate, error) {
	// 09:04:16.717000,38.19,2781,38.25,3308,38.21,638,Trading
	update := new(MarketUpdate)
	if len(records) != 8 {
		return nil, errors.New("Must have 8 fields")
	}

	str := records[0]
	// timestamp: 19:30:41.977000
	if len(str) < len(stampFormat) {
		return nil, errors.New("timestamp too short should be " + stampFormat)
	}
	var err error
	var hour, min, sec, us int
	if hour, err = strconv.Atoi(str[0:2]); err != nil {
		return nil, errors.New("cannot parse HH in " + stampFormat)
	}
	if min, err = strconv.Atoi(str[3:5]); err != nil {
		return nil, errors.New("cannot parse MM in " + stampFormat)
	}
	if sec, err = strconv.Atoi(str[6:8]); err != nil {
		return nil, errors.New("cannot parse SS in " + stampFormat)
	}
	if us, err = strconv.Atoi(str[9:15]); err != nil {
		return nil, errors.New("cannot parse XXXXXX in " + stampFormat)
	}
	update.Timestamp = time.Date(2010, time.December, 16, hour, min,
		sec, us*1000, time.Local)

	// 38.19 2781 38.25 3308,38.21,638,Trading
	if update.Bid.Price, err = strconv.ParseFloat(records[1], 64); err != nil {
		return nil, errors.New("cannot parse bid (field 2)")
	}
	if update.Bid.Size, err = strconv.ParseInt(records[2], 10, 64); err != nil {
		return nil, errors.New("cannot parse bidsize (field 3)")
	}
	if update.Ask.Price, err = strconv.ParseFloat(records[3], 64); err != nil {
		return nil, errors.New("cannot parse ask (field 4)")
	}
	if update.Ask.Size, err = strconv.ParseInt(records[4], 10, 64); err != nil {
		return nil, errors.New("cannot parse asksize (field 5)")
	}
	if update.Last.Price, err = strconv.ParseFloat(records[5], 64); err != nil {
		return nil, errors.New("cannot parse last (field 6)")
	}
	if update.Last.Size, err = strconv.ParseInt(records[6], 10, 64); err != nil {
		return nil, errors.New("cannot parse asksize (field 7)")
	}
	return update, nil
}

func treatUpdate(update *MarketUpdate) {
     fmt.Printf("update: %v\n", update)
}
