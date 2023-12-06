package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/net/html/charset"
)

type Site struct {
	Url             string   `json:"url"`
	State           string   `json:"state"`
	Categories      []string `json:"categories"`
	ForMainPage     bool     `json:"for_main_page"`
	CategoryAnother *string  `json:"category_another"`
	Ctime           int64    `json:"ctime"`
}

type FileWriter struct {
	Writer *bufio.Writer
	File   *os.File
}

type parser struct {
	client         *http.Client
	requestBuilder func(url string) (*http.Request, error)
}

type Crawler struct {
	mu           sync.Mutex
	parser       *parser
	fw           map[string]*FileWriter
	sitesChan    chan *Site
	meg          multierror.Group
	checkCounter uint32
	workers      uint16
	Stop         chan struct{}
}

func NewCrawler(timeout time.Duration, workers uint16, insecure bool) *Crawler {
	return &Crawler{
		sitesChan: make(chan *Site, 100),
		fw:        make(map[string]*FileWriter),
		workers:   workers,
		parser: &parser{
			client: &http.Client{
				Timeout: timeout,
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: insecure,
					},
				},
			},
			requestBuilder: func(url string) (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, url, nil)
				if err != nil {
					return nil, err
				}

				req.Close = true
				req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

				return req, nil
			},
		},
		Stop: make(chan struct{}),
	}
}

func NewFileWriter(filename string) (*FileWriter, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(file)

	return &FileWriter{
		File:   file,
		Writer: writer,
	}, nil
}

func (c *Crawler) LoadSitesFromFile(filepath string) error {
	file, err := os.Open(filepath)
	wg := &sync.WaitGroup{}
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	for decoder.More() {
		wg.Add(1)
		var site *Site
		err = decoder.Decode(&site)
		if err != nil {
			return err
		}
		go func(site *Site) {
			defer wg.Done()
			c.sitesChan <- site
		}(site)
	}
	go func() {
		wg.Wait()
		close(c.sitesChan)
	}()

	return nil
}

func (c *Crawler) PrintStatus() {
	log.Printf("Checked %d sites\n", atomic.LoadUint32(&c.checkCounter))
}

func (c *Crawler) Start() {
	go c.CheckSites()
}

func (c *Crawler) CheckSites() {
	semaphore := make(chan struct{}, c.workers)
	for site := range c.sitesChan {
		semaphore <- struct{}{}
		site := site
		c.meg.Go(func() error {
			defer func() {
				<-semaphore
			}()
			req, err := c.parser.requestBuilder(site.Url)
			if err != nil {
				return err
			}
			resp, err := c.parser.client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			atomic.AddUint32(&c.checkCounter, 1)

			if resp.StatusCode != http.StatusOK {
				return err
			}

			reader, err := charset.NewReader(resp.Body, resp.Header.Get("Content-Type"))
			if err != nil {
				return err
			}
			doc, err := goquery.NewDocumentFromReader(reader)
			if err != nil {
				return err
			}

			title := doc.Find("title").Text()
			description := doc.Find("meta[name=description]").AttrOr("content", "")

			c.mu.Lock()
			defer c.mu.Unlock()

			for _, category := range site.Categories {
				if _, ok := c.fw[category]; !ok {
					fw, fwErr := NewFileWriter("./" + category + ".tsv")
					if fwErr != nil {
						return fwErr
					}
					c.fw[category] = fw
				}
				line := fmt.Sprintf("%s\t%s\t%s\n", site.Url, title, description)
				if _, fwErr := c.fw[category].Writer.WriteString(line); fwErr != nil {
					return fwErr
				}
			}

			return nil
		})
	}

	mErr := c.meg.Wait()
	if mErr != nil {
		log.Printf(mErr.Error())
	}

	for _, fw := range c.fw {
		if err := fw.Writer.Flush(); err != nil {
			log.Printf(err.Error())
		}
		if err := fw.File.Close(); err != nil {
			log.Printf(err.Error())
		}
	}

	close(c.Stop)
}

func main() {
	parser := NewCrawler(10*time.Second, 30, true)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	err := parser.LoadSitesFromFile("./500.jsonl")
	if err != nil {
		panic(err)
	}
	parser.Start()

	for {
		select {
		case <-parser.Stop:
			return
		case <-ticker.C:
			parser.PrintStatus()
		}
	}
}
