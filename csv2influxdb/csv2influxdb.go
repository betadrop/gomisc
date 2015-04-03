package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"github.com/influxdb/influxdb/client"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type info struct {
	ticker           string
	year, month, day int
	gz               bool
}

var parseError error = errors.New("filename must be of format CSGN_2010-12-16.csv[.gz]")

func parse(filename string) (info info, err error) {
	var pos int
	if pos = strings.LastIndex(filename, "/"); pos != -1 {
		filename = filename[pos+1:]
	}
	if pos = strings.Index(filename, "_"); pos == -1 {
		return info, parseError
	}
	info.ticker = filename[0:pos]
	filename = filename[pos+1:]
	if info.year, err = strconv.Atoi(filename[0:4]); err != nil {
		return info, parseError
	}
	filename = filename[5:]
	if info.month, err = strconv.Atoi(filename[0:2]); err != nil {
		return info, parseError
	}
	filename = filename[3:]
	if info.day, err = strconv.Atoi(filename[0:2]); err != nil {
		return info, parseError
	}
	info.gz = strings.HasSuffix(filename, ".gz")
	return info, nil
}

func main() {
	var host string
	flag.StringVar(&host, "host", "localhost:8086", "which influx host:port to connect to")
	var count int
	flag.IntVar(&count, "count", 100, "how many item to process")
	flag.Parse()
	args := os.Args
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage:", args[0], "file")
		os.Exit(1)
	}
	filename := flag.Arg(0)
	info, err := parse(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	fmt.Println(info)
	clt, err = connect(host)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot connect to influxdb on %v:\n\t%v\n", host, err)
		os.Exit(1)
	}
	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintln(os.Stderr, "reading "+filename+":", err)
		os.Exit(1)
	}
	reader := csv.NewReader(bufio.NewReader(file))
	var lineNum int
	for {
		records, err := reader.Read()
		lineNum = lineNum + 1
		if records != nil {
			update, err := ReadUpdate(info, records)
			if err == nil {
				if treatUpdate("CSGN", update) {
					count = count - 1
					if count == 0 {
						break
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
	flush("CSGN")
}

const (
	stampFormat = "HH:MM:SS.XXXXXX"
)

var clt *client.Client

func connect(host string) (*client.Client, error) {
	cfg := client.Config{
		URL: url.URL{
			Scheme: "http",
			Host:   host}}
	c, err := client.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	time, v, err := c.Ping()
	if err != nil {
		return nil, err
	}
	fmt.Printf("Ping in %v. Version: %v\n", time, v)
	return c, nil
}

func write(c *client.Client, ticker string, updates []MarketUpdate) error {
	var points []client.Point
	for _, update := range updates {
		fields := make(map[string]interface{})
		fields["bid"] = update.Bid.Price
		fields["bidsize"] = update.Bid.Size
		fields["ask"] = update.Ask.Price
		fields["asksize"] = update.Ask.Size
		fields["last"] = update.Last.Price
		fields["lastsize"] = update.Last.Size
		first := client.Point{
			Name:      ticker,
			Timestamp: update.Timestamp,
			Fields:    fields,
		}
		points = append(points, first)
	}
	bp := client.BatchPoints{
		Points:   points,
		Database: "marketdata"}
	res, err := c.Write(bp)
	if err != nil {
		return err
	}
	fmt.Printf("Results: %v\n", res)
	return nil
}

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

var batch []MarketUpdate
var current time.Time

func treatUpdate(ticker string, update *MarketUpdate) (ok bool) {
	//fmt.Println("update:", *update)
	if update.Timestamp.After(current) {
		batch = append(batch, *update)
		current = update.Timestamp
		if len(batch) == 50 {
			sendBatch(ticker)
		}
		return true
	}
	return false
}

func flush(ticker string) {
	if len(batch) > 0 {
		sendBatch(ticker)
	}
}

func sendBatch(ticker string) {
	if len(batch) > 0 {
		err := write(clt, ticker, batch)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Cannot write to influxdb", err)
			os.Exit(1)
		} else {
			fmt.Fprintf(os.Stdout, "Wrote %v OK\n", len(batch))
		}
		batch = nil
	}
}
