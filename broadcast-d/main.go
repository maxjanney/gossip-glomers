package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	var mu sync.Mutex
	c := make(chan struct{})
	ticker := time.NewTicker(600 * time.Millisecond)
	neighbors := make([]string, 0, 5)
	known := make(map[string]map[int]struct{})
	state := make(map[int]struct{})
	node := maelstrom.NewNode()

	go func() {
		for {
			select {
			case <-c:
				ticker.Stop()
				return 
			case <-ticker.C:
				for _, n := range neighbors {
					mu.Lock()
					messages := make([]int, 0, len(state))
					for m := range state {
						if _, ok := known[n][m]; ok {
							continue
						}
						messages = append(messages, m)
					}
					mu.Unlock()
					body := map[string]any{"type": "gossip", "messages": messages}
					node.Send(n, body)
				}
			}
		}
	}()

	node.Handle("init", func(msg maelstrom.Message) error {
		for _, n := range node.NodeIDs() {
			if n == node.ID() {
				continue
			}
			known[n] = make(map[int]struct{})
			neighbors = append(neighbors, n)
		}
		return nil
	})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		respBody := map[string]any{"type": "broadcast_ok"}
		node.Reply(msg, respBody)
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		m := int(body["message"].(float64))
		mu.Lock()
		if _, ok := state[m]; ok {
			mu.Unlock()
			return nil
		}
		state[m] = struct{}{}
		mu.Unlock()
		return nil
	})

	node.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		messages := body["messages"].([]any)
		for _, m := range messages {
			i := int(m.(float64))
			known[msg.Src][i] = struct{}{}
			if _, ok := state[i]; ok {
				continue 
			}
			state[i] = struct{}{}
		}
		mu.Unlock()
		return nil 
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		messages := make([]int, 0, len(state))
		for m := range state {
			messages = append(messages, m)
		}
		mu.Unlock()
		body := map[string]any{"type": "read_ok", "messages": messages}
		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		return node.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
		c<-struct{}{}
	}
}
