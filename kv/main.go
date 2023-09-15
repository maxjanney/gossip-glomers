package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	var mu sync.Mutex
	node := maelstrom.NewNode()
	kv := make(map[int]int)

	node.Handle("txn", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err 
		}
		mu.Lock()
		txn := body["txn"].([]any)
		for _, operation := range txn {
			op := operation.([]any)
			ty := op[0].(string)
			key := int(op[1].(float64))
			if ty == "r" {
				if val, ok := kv[key]; ok {
					op[2] = val 
				}
			} else if ty == "w" {
				kv[key] = int(op[2].(float64))
			}
		}
		body["type"] = "txn_ok"
		mu.Unlock()
		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}