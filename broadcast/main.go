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
	var ackMu sync.Mutex
	neighbors := make([]string, 0, 5)
	state := make(map[int]struct{})
	node := maelstrom.NewNode()

	node.Handle("init", func(msg maelstrom.Message) error {
		for _, n := range node.NodeIDs() {
			if n == node.ID() {
				continue
			}
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

		unack := createUnack(neighbors, msg.Src)
		for len(unack) > 0 {
			for n := range unack {
				rpcBody := map[string]any{"type": "broadcast", "message": m}
				node.RPC(n, rpcBody, func(msg maelstrom.Message) error {
					var body map[string]any
					if err := json.Unmarshal(msg.Body, &body); err != nil {
						return err
					}
					ackMu.Lock()
					if _, ok := unack[n]; ok && body["type"] == "broadcast_ok" {
						delete(unack, n)
					}
					ackMu.Unlock()
					return nil
				})
			}
			time.Sleep(time.Second)
		}
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

	node.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func createUnack(neighbors []string, src string) map[string]struct{} {
	unack := make(map[string]struct{})
	for _, n := range neighbors {
		if n == src {
			continue
		}
		unack[n] = struct{}{}
	}
	return unack
}
