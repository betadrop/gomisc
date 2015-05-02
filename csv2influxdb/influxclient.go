package main

import (
	"fmt"
	"github.com/influxdb/influxdb/client"
	"net/url"
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
