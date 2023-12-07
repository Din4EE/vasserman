package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func main() {
	var result string
	inputData := []int{0, 1}
	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
		job(func(in, out chan interface{}) {
			dataRaw := <-in
			data, _ := dataRaw.(string)
			result = data
		}),
	}
	ExecutePipeline(hashSignJobs...)
	fmt.Println(result)
}

func ExecutePipeline(freeFlowJobs ...job) {
	var wg sync.WaitGroup
	in := make(chan interface{})
	for _, funcWorkers := range freeFlowJobs {
		out := make(chan interface{})
		wg.Add(1)
		go func(in chan interface{}, out chan interface{}, wg *sync.WaitGroup, workers job) {
			defer wg.Done()
			defer close(out)
			workers(in, out)
		}(in, out, &wg, funcWorkers)
		in = out
	}
	wg.Wait()
}

func getAlgo(ch chan string, data string) {
	ch <- DataSignerCrc32(data)
}

func SingleHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	var wgAlgo sync.WaitGroup
	crc32Receiver := make(chan string)
	crc32WithMd5Receiver := make(chan string)
	for v := range in {
		wg.Add(1)
		data := v.(int)
		md5 := DataSignerMd5(strconv.Itoa(v.(int)))
		go func(wg *sync.WaitGroup, wgAdditional *sync.WaitGroup, out chan interface{}, data int, hash string) {
			defer wg.Done()
			wgAdditional.Add(1)
			go getAlgo(crc32Receiver, strconv.Itoa(data))
			go getAlgo(crc32WithMd5Receiver, hash)
			wgAdditional.Done()
			crc32 := <-crc32Receiver
			crc32WithMd5 := <-crc32WithMd5Receiver
			wgAdditional.Wait()
			result := crc32 + "~" + crc32WithMd5
			fmt.Printf("%v SingleHash data %v\n", data, data)
			fmt.Printf("%v SingleHash md5(data) %v\n", data, md5)
			fmt.Printf("%v SingleHash crc32(md5(data)) %v\n", data, crc32WithMd5)
			fmt.Printf("%v SingleHash crc32(data) %v\n", data, crc32)
			fmt.Printf("%v SingleHash result %v\n", data, result)
			out <- result
		}(&wg, &wgAlgo, out, data, md5)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	var wgThreads sync.WaitGroup
	for v := range in {
		wg.Add(1)
		data := v.(string)
		go func(wg *sync.WaitGroup, out chan interface{}, data string) {
			defer wg.Done()
			var result string
			maxThreads := 6
			threadsSlice := make([]string, maxThreads, maxThreads)
			for i := 0; i < maxThreads; i++ {
				thread := i
				wgThreads.Add(1)
				go func(wg *sync.WaitGroup, th int, data string, threadSlice []string) {
					defer wg.Done()
					threadsSlice[th] = DataSignerCrc32(strconv.Itoa(th) + data)
					fmt.Printf("%v MultiHash: crc32(th+step1) %v %v\n", data, thread, threadsSlice[th])
				}(&wgThreads, thread, data, threadsSlice)
			}
			wgThreads.Wait()
			result = strings.Join(threadsSlice, "")
			out <- result
		}(&wg, out, data)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var result string
	var sl []string
	for v := range in {
		hash := v.(string)
		sl = append(sl, hash)
	}
	sort.Strings(sl)
	result = strings.Join(sl, "_")
	fmt.Println("CombineResults", result)
	out <- result
}

//func SingleHashSync(data int) string {
//	md5 := DataSignerMd5(strconv.Itoa(data))
//	crc32WithMd5 := DataSignerCrc32(md5)
//	crc32 := DataSignerCrc32(strconv.Itoa(data))
//	result := crc32 + "~" + crc32WithMd5
//	fmt.Printf("%v SingleHash data %v\n", data, data)
//	fmt.Printf("%v SingleHash md5(data) %v\n", data, md5)
//	fmt.Printf("%v SingleHash crc32(md5(data)) %v\n", data, crc32WithMd5)
//	fmt.Printf("%v SingleHash crc32(data) %v\n", data, crc32)
//	fmt.Printf("%v SingleHash result %v\n", data, result)
//	return result
//}
//
//func MultiHashSync(hash string) string {
//	var result string
//	for i := 0; i < 6; i++ {
//		resultForIter := DataSignerCrc32(strconv.Itoa(i) + hash)
//		fmt.Printf("%v MultiHash: crc32(th+step1) %v %v\n", hash, i, resultForIter)
//		result += resultForIter
//	}
//	fmt.Printf("%v, MultiHash result: %v\n", hash, result)
//	return result
//}
//
//func CombineResultsSync(hash ...string) {
//	var result string
//	var sl []string
//	sl = append(sl, hash...)
//	sort.Strings(sl)
//	for _, v := range sl {
//		result += v + "_"
//	}
//	fmt.Println("CombineResults", result[:len(result)-1])
//}
