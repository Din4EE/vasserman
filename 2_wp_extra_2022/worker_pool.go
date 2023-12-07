package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/cpu"
)

const cpuPercentTrigger = 2

type WorkerPool struct {
	maxWorkers     int32
	workersCounter int32
	workerChan     chan struct{}
	wg             sync.WaitGroup
}

func NewWorkerPool(maxWorkers int32) *WorkerPool {
	return &WorkerPool{
		maxWorkers: maxWorkers,
		workerChan: make(chan struct{}),
	}
}

func (wp *WorkerPool) StartWorker() {
	atomic.AddInt32(&wp.workersCounter, 1)
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		defer atomic.AddInt32(&wp.workersCounter, -1)
		for {
			select {
			case <-wp.workerChan:
				log.Printf("Worker stopped")
				return
			}
		}
	}()
}

func (wp *WorkerPool) StopWorker() {
	wp.workerChan <- struct{}{}
}

func (wp *WorkerPool) Down() {
	close(wp.workerChan)
	wp.wg.Wait()
}

func (wp *WorkerPool) AdjustWorkers() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf("Current workers count: %d\n", atomic.LoadInt32(&wp.workersCounter))
			percent, _ := cpu.Percent(time.Second, false)
			currentLoad := percent[0]
			log.Printf("Current CPU load: %.2f%%\n", currentLoad)
			if currentLoad > cpuPercentTrigger && wp.maxWorkers > atomic.LoadInt32(&wp.workersCounter) {
				log.Println("Add worker")
				wp.StartWorker()
			} else if currentLoad < cpuPercentTrigger && int(atomic.LoadInt32(&wp.workersCounter)) > 1 {
				log.Println("Remove worker")
				wp.StopWorker()
			}
		}
	}
}

func main() {
	wp := NewWorkerPool(10)
	go wp.AdjustWorkers()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	log.Printf("Got interrupt signal: %v\n", <-c)

	wp.Down()

	log.Println("All workers stopped")
}
