package main

import (
	"fmt"
	"github.com/influxdb/influxdb/client"
	"net/url"
	"os"
)

type Point client.Point

type Client struct {
	clt     *client.Client
	db      string
	version string
	in      chan Point
	done    chan bool
}

// Return a new client and connect (ping) to it
func NewClient(host string, db string) (*Client, error) {
	c := new(Client)
	var err error
	cfg := client.Config{
		URL: url.URL{
			Scheme: "http",
			Host:   host}}
	c.clt, err = client.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	c.version, err = c.Ping()
	if err != nil {
		return nil, err
	}
	c.db = db
	c.in = make(chan Point)
	c.done = make(chan bool)
	go c.loop()
	return c, nil
}

func (c *Client) Ping() (version string, err error) {
	time, v, err := c.clt.Ping()
	if err != nil {
		return "", err
	}
	fmt.Printf("Ping in %v. Version: %v\n", time, v)
	return v, nil
}

func (c *Client) Write(point Point) {
	c.in <- point
}

func (c *Client) Close() {
	close(c.in)
}

func (c *Client) loop() {
	var points []client.Point
	var written int
	for {
		point, ok := <-c.in
		fmt.Printf("Point: %v\n", point)

		if ok {
			points = append(points, client.Point(point))
			if len(points) == 50 {
				err := c.write(points)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Cannot write: %v\n", err)
				} else {
					written = written + len(points)
				}
				points = nil
			}
		} else { // channel closed
			if len(points) > 0 {
				err := c.write(points)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Cannot write: %v\n", err)
				} else {
					written = written + len(points)
				}
			}
			fmt.Printf("Finished %v update\n", written)
			c.done <- true
			break
		}
	}
}

func (c *Client) write(points []client.Point) error {
	bp := client.BatchPoints{
		Points:   points,
		Database: c.db}
	res, err := c.clt.Write(bp)
	if err != nil {
		return err
	}
	fmt.Printf("Results: %v\n", res)
	return nil
}
