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

type DataWriter interface {
	Write(data string) error
	Flush() error
	Close() error
}

type ConsoleWriter struct {
	Writer *bufio.Writer
}

type FileWriter struct {
	Writer *bufio.Writer
	File   *os.File
}

type parser struct {
	client         *http.Client
	requestBuilder func(url string) (*http.Request, error)
	rateLimit      <-chan time.Time
}

type Crawler struct {
	mu           sync.Mutex
	parser       *parser
	meg          multierror.Group
	wg           sync.WaitGroup
	checkCounter uint32
	writerType   string
}

func NewCrawler(timeout time.Duration, rps uint64, insecure bool, writerType string) (*Crawler, error) {

	if rps <= 0 {
		return nil, fmt.Errorf("rps cannot be %d", rps)
	}

	return &Crawler{
		writerType: writerType,
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
			rateLimit: time.Tick(time.Second / time.Duration(rps)),
		},
	}, nil
}

func NewFileWriter(filename string) (DataWriter, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &FileWriter{
		Writer: bufio.NewWriter(file),
		File:   file,
	}, nil
}

func NewConsoleWriter() (DataWriter, error) {
	return &ConsoleWriter{
		Writer: bufio.NewWriter(os.Stdout),
	}, nil
}

func (fw *FileWriter) Write(data string) error {
	_, err := fw.Writer.WriteString(data)
	return err
}

func (fw *FileWriter) Flush() error {
	return fw.Writer.Flush()
}

func (fw *FileWriter) Close() error {
	return fw.File.Close()
}

func (cw *ConsoleWriter) Write(data string) error {
	_, err := cw.Writer.WriteString(fmt.Sprintf("%s\n", data))
	return err
}

func (cw *ConsoleWriter) Flush() error {
	return cw.Writer.Flush()
}

func (cw *ConsoleWriter) Close() error {
	return nil
}

func (c *Crawler) loadSitesFromFile(filepath string) (chan *Site, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	sitesChan := make(chan *Site)
	decoder := json.NewDecoder(file)
	for decoder.More() {
		c.wg.Add(1)
		var site *Site
		err = decoder.Decode(&site)
		if err != nil {
			return nil, err
		}
		go func(site *Site) {
			defer c.wg.Done()
			sitesChan <- site
		}(site)
	}
	go func() {
		c.wg.Wait()
		close(sitesChan)
	}()

	return sitesChan, nil
}

func (c *Crawler) printStatus() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf("Checked %d sites", atomic.LoadUint32(&c.checkCounter))
		}
	}
}

func (c *Crawler) Start(filepath string) error {
	sitesChan, err := c.loadSitesFromFile(filepath)
	if err != nil {
		return err
	}
	c.checkSites(sitesChan)

	return nil
}

func (c *Crawler) checkSites(sitesChan <-chan *Site) {
	wMap := make(map[string]DataWriter)
	for site := range sitesChan {
		site := site
		c.meg.Go(func() error {
			<-c.parser.rateLimit
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
			if description == "" {
				description = doc.Find("meta[property='og:description']").AttrOr("content", "")
			}

			c.mu.Lock()
			defer c.mu.Unlock()

			for _, category := range site.Categories {
				if _, ok := wMap[category]; !ok {
					wMap[category], err = c.createWriterForCategory(category)
					if err != nil {
						return err
					}
				}
				line := fmt.Sprintf("%s\t%s\t%s\n", site.Url, title, description)
				if wErr := wMap[category].Write(line); wErr != nil {
					return wErr
				}
			}

			return nil
		})
	}

	mErr := c.meg.Wait()
	for _, w := range wMap {
		if err := w.Flush(); err != nil {
			log.Printf(err.Error())
		}
		if err := w.Close(); err != nil {
			log.Printf(err.Error())
		}
	}
	if mErr != nil {
		log.Printf(mErr.Error())
	}
}

func (c *Crawler) createWriterForCategory(category string) (DataWriter, error) {
	switch c.writerType {
	case "file":
		return NewFileWriter(fmt.Sprintf("%s.tsv", category))
	default:
		return NewConsoleWriter()
	}
}

func main() {
	crawler, err := NewCrawler(10*time.Second, 30, true, "")
	if err != nil {
		log.Fatalf(err.Error())
	}
	if err = crawler.Start("./500.jsonl"); err != nil {
		log.Fatalf(err.Error())
	}
}
