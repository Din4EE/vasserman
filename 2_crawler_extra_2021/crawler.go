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
	sites        []*Site
	meg          multierror.Group
	checkCounter uint32
}

func NewParser(timeout time.Duration, insecure bool) *Parser {
	return &Parser{
		sites: make([]*Site, 0),
		fw:    make(map[string]*FileWriter),
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: insecure,
				},
			},
		},
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
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for decoder.More() {
		var site *Site
		err = decoder.Decode(&site)
		if err != nil {
			return err
		}
		p.sites = append(p.sites, site)
	}

	log.Printf("Loaded %d sites\n", len(p.sites))
	return nil
}

func (p *Parser) PrintStatus() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf("Checked %d sites of %d\n", atomic.LoadUint32(&p.checkCounter), len(p.sites))
		}
	}
}

func (p *Parser) CheckSites() {
	for _, site := range p.sites {
		site := site
		p.meg.Go(func() error {
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
}

func main() {
	parser := NewParser(30*time.Second, true)
	err := parser.LoadSitesFromFile("./500.jsonl")
	if err != nil {
		panic(err)
	}
	go parser.PrintStatus()
	parser.CheckSites()
}
