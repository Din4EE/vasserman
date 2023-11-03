package __async_2023

import (
	"fmt"
	"log"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	in := make(chan interface{})
	wg := &sync.WaitGroup{}
	for _, c := range cmds {
		wg.Add(1)
		out := make(chan interface{})
		go func(c cmd, in, out chan interface{}) {
			defer wg.Done()
			defer close(out)
			c(in, out)
		}(c, in, out)
		in = out
	}
	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	processedUsers := make(map[uint64]struct{})

	for v := range in {
		wg.Add(1)
		go func(email string) {
			defer wg.Done()
			defer mu.Unlock()
			user := GetUser(email)

			mu.Lock()
			if _, ok := processedUsers[user.ID]; ok {
				return
			}
			processedUsers[user.ID] = struct{}{}

			out <- user
		}(v.(string))
	}
	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	userBatch := make([]User, 0, GetMessagesMaxUsersBatch)
	for user := range in {
		userBatch = append(userBatch, user.(User))

		if len(userBatch) == GetMessagesMaxUsersBatch {
			wg.Add(1)
			go func(batch []User) {
				defer wg.Done()
				msgIDs, err := GetMessages(batch...)
				if err != nil {
					log.Printf("error: %v", err)
					return
				}
				for _, msgID := range msgIDs {
					out <- msgID
				}
			}(userBatch)
			userBatch = make([]User, 0, GetMessagesMaxUsersBatch)
		}
	}

	if len(userBatch) > 0 {
		wg.Add(1)
		go func(batch []User) {
			defer wg.Done()
			msgIDs, err := GetMessages(batch...)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			for _, msgID := range msgIDs {
				out <- msgID
			}
		}(userBatch)
	}
	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	done := make(chan struct{}, HasSpamMaxAsyncRequests)
	wg := &sync.WaitGroup{}
	for msgID := range in {
		id := msgID.(MsgID)

		done <- struct{}{}
		wg.Add(1)

		go func(id MsgID) {
			defer func() {
				<-done
				wg.Done()
			}()
			isSpam, err := HasSpam(id)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			out <- MsgData{ID: id, HasSpam: isSpam}
		}(id)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var results []MsgData
	for msgData := range in {
		results = append(results, msgData.(MsgData))
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam != results[j].HasSpam {
			return results[i].HasSpam
		}
		return results[i].ID < results[j].ID
	})
	for _, result := range results {
		out <- fmt.Sprintf("%t %d", result.HasSpam, result.ID)
	}
}
