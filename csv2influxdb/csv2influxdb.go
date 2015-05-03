package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type info struct {
	ticker           string
	year, month, day int
	gz               bool
}

var batch []MarketUpdate
var current time.Time

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

var clt Client

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
	var c *Client
	if !params.dryRun {
		c, err = NewClient(params.host, params.db)
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
		if records != nil {
			update, err := ReadUpdate(info, records)
			if err == nil {
				if treatUpdate(c, "CSGN", update) {
					if params.count > 0 {
						params.count = params.count - 1
						if params.count == 0 {
							break
						}
					}
				}
			} else {
				fmt.Fprintln(os.Stderr, "line", lineNum, "error:", err, records)
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
	if c != nil {
		c.Close()
		<-c.done
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

func (update *MarketUpdate) getFields() map[string]interface{} {
	fields := make(map[string]interface{})
	fields["bid"] = update.Bid.Price
	//fields["bidsize"] = strconv.FormatInt(update.Bid.Size, 10)
	fields["bidsize"] = update.Bid.Size
	fields["ask"] = update.Ask.Price
	fields["asksize"] = update.Ask.Size
	fields["last"] = update.Last.Price
	fields["lastsize"] = update.Last.Size
	return fields
}

func ReadUpdate(info info, records []string) (*MarketUpdate, error) {
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
	update.Timestamp = time.Date(info.year, time.Month(info.month), info.day, hour, min,
		sec, us*1000, time.Local)

	// 38.19,2781,38.25,3308,38.21,638,Trading
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
		return nil, errors.New("cannot parse lastsize (field 7)")
	}
	return update, nil
}

func treatUpdate(c *Client, ticker string, update *MarketUpdate) (ok bool) {
	// Ignore duplicate time. Just take the first for now.
	if update.Timestamp.After(current) {
		current = update.Timestamp
		if c != nil {
			c.Write(Point{
				Name:      ticker,
				Timestamp: update.Timestamp,
				Fields:    update.getFields()})
		}
		return true
	}
	return false
}
