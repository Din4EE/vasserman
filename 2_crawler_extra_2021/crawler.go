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

type Parser struct {
	mu           sync.Mutex
	client       *http.Client
	fw           map[string]*FileWriter
	sitesChan    chan *Site
	meg          multierror.Group
	checkCounter uint32
	workers      uint16
	Stop         chan struct{}
}

func NewParser(timeout time.Duration, workers uint16, insecure bool) *Parser {
	return &Parser{
		sitesChan: make(chan *Site, 100),
		fw:        make(map[string]*FileWriter),
		workers:   workers,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: insecure,
				},
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

func (p *Parser) LoadSitesFromFile(filepath string) error {
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
			p.sitesChan <- site
		}(site)
	}
	go func() {
		wg.Wait()
		close(p.sitesChan)
	}()
	return nil
}

func (p *Parser) PrintStatus() {
	log.Printf("Checked %d sites\n", atomic.LoadUint32(&p.checkCounter))
}

func (p *Parser) Start() {
	go p.CheckSites()
}

func (p *Parser) CheckSites() {
	semaphore := make(chan struct{}, p.workers)
	for site := range p.sitesChan {
		semaphore <- struct{}{}
		site := site
		p.meg.Go(func() error {
			defer func() {
				<-semaphore
			}()
			req, err := http.NewRequest(http.MethodGet, site.Url, nil)
			if err != nil {
				return err
			}
			resp, err := p.client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return err
			}

			atomic.AddUint32(&p.checkCounter, 1)

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

			p.mu.Lock()
			defer p.mu.Unlock()

			for _, category := range site.Categories {
				if _, ok := p.fw[category]; !ok {
					fw, fwErr := NewFileWriter("./" + category + ".tsv")
					if fwErr != nil {
						return fwErr
					}
					p.fw[category] = fw
				}
				line := fmt.Sprintf("%s\t%s\t%s\n", site.Url, title, description)
				if _, fwErr := p.fw[category].Writer.WriteString(line); fwErr != nil {
					return fwErr
				}
			}

			return nil
		})
	}

	mErr := p.meg.Wait()
	if mErr != nil {
		log.Printf(mErr.Error())
	}

	for _, fw := range p.fw {
		if err := fw.Writer.Flush(); err != nil {
			log.Printf(err.Error())
		}
		if err := fw.File.Close(); err != nil {
			log.Printf(err.Error())
		}
	}

	close(p.Stop)
}

func main() {
	parser := NewParser(10*time.Second, 30, true)
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
