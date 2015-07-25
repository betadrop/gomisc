package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"github.com/influxdb/influxdb/client"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type info struct {
	ticker           string
	year, month, day int
	gz               bool
}

var parseError error = errors.New("filename must be of format CSGN_2010-12-16.csv[.gz]")

// CSGN_2010-12-16.csv[.gz]
var fileRegexp *regexp.Regexp = regexp.MustCompile(`(.+)_([0-9]{4})-([0-9]{2})-([0-9]{2})\.csv(\.gz)?$`)

func parse(filename string) (info info, err error) {
	var pos int
	if pos = strings.LastIndex(filename, "/"); pos != -1 {
		filename = filename[pos+1:]
	}
	matches := fileRegexp.FindStringSubmatch(filename)
	if matches == nil || len(matches) != 6 {
		return info, parseError
	}
	info.ticker = matches[1]
	if info.year, err = strconv.Atoi(matches[2]); err != nil {
		return info, parseError
	}
	if info.month, err = strconv.Atoi(matches[3]); err != nil {
		return info, parseError
	}
	if info.day, err = strconv.Atoi(matches[4]); err != nil {
		return info, parseError
	}
	info.gz = matches[5] == ".gz"

	return info, nil
}

var params struct {
	host    string
	db      string
	count   int
	verbose bool
	dryRun  bool
}

func init() {
	flag.StringVar(&params.host, "host", "localhost:8086", "which influx host:port to connect to")
	flag.StringVar(&params.db, "db", "marketdata", "which influx database to use")
	flag.IntVar(&params.count, "count", -1, "how many item to process")
	flag.BoolVar(&params.dryRun, "dry-run", false, "do not send the item to influx")
	flag.Parse()
	fmt.Printf("%+v\n", params)
}

func main() {
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage:", os.Args[0], "file")
		os.Exit(1)
	}
	filename := flag.Arg(0)
	info, err := parse(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	fmt.Println(info)
	var clt *Client
	if !params.dryRun {
		clt, err = NewClient(params.host, params.db)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot connect to influxdb on %v:\n\t%v\n", params.host, err)
			os.Exit(1)
		}
	}
	var file io.Reader
	file, err = os.Open(filename)
	if err != nil {
		fmt.Fprintln(os.Stderr, "reading "+filename+":", err)
		os.Exit(1)
	}
	if info.gz == true {
		file, err = gzip.NewReader(file)
		if err != nil {
			fmt.Fprintln(os.Stderr, "gunzip "+filename+":", err)
			os.Exit(1)
		}
	}
	reader := csv.NewReader(bufio.NewReader(file))
	var lineNum int
	for {
		records, err := reader.Read()
		lineNum = lineNum + 1
		if err != nil {
			if err != io.EOF {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			break
		}
		if records != nil {
			timestamp, fields, err := parseFields(info, records)
			if err != nil {
				fmt.Fprintln(os.Stderr, "line", lineNum, "error:", err, records)
				continue
			}
			if fields != nil {
				if clt != nil {
					clt.Write(&client.Point{
						Name:      "CSGN",
						Timestamp: timestamp,
						Fields:    fields})
				}
			}
		}
	}
	if clt != nil {
		clt.Close()
	}
}

const (
	stampFormat = "HH:MM:SS.XXXXXX"
)
