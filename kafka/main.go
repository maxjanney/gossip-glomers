package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	var logMu sync.Mutex
	var indxMu sync.Mutex
	uncommitted := make(map[string]int)
	committed := make(map[string]int)
	logs := make(map[string][][2]int)
	node := maelstrom.NewNode()

	node.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"].(string)
		val := int(body["msg"].(float64))
		logMu.Lock()
		offset := uncommitted[key]
		uncommitted[key]++
		logs[key] = append(logs[key], [2]int{offset, val})
		logMu.Unlock()
		return node.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		msgs := make(map[string][][2]int)
		offsets := body["offsets"].(map[string]any)
		logMu.Lock()
		for k, v := range offsets {
			for _, l := range logs[k] {
				val := int(v.(float64))
				if l[0] >= val {
					msgs[k] = append(msgs[k], l)
				}
			}
		}
		logMu.Unlock()
		replyBody := map[string]any{"type": "poll_ok", "msgs": msgs}
		return node.Reply(msg, replyBody)
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]any)
		indxMu.Lock()
		for k, v := range offsets {
			committed[k] = int(v.(float64))
		}
		indxMu.Unlock()
		return node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
				return err
		}
		keys := body["keys"].([]any)
		offsets := make(map[string]int)
		indxMu.Lock()
		for _, k := range keys {
			key := k.(string)
			if _, ok := committed[key]; !ok {
				continue
			}
			offsets[key] = committed[key]
		}
		indxMu.Unlock()
		return node.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}