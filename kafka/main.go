package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(node)

	node.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"].(string)
		val := int(body["msg"].(float64))
		uKey := "u" + key
		offset, err := kv.ReadInt(context.Background(), uKey)
		if err != nil {
			rpcError := err.(* maelstrom.RPCError)
			if rpcError.Code != maelstrom.KeyDoesNotExist {
				return err 
			}
			offset = 0
		} else {
			offset++
		}
		for {
			if err := kv.CompareAndSwap(context.Background(), uKey, offset-1, offset, true); err == nil {
				break
			}
			offset++
		}
		if err := kv.Write(context.Background(), fmt.Sprintf("%d_%s", offset, key), val); err != nil {
			return err
		}
		return node.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]any)
		msgs := make(map[string][][2]int)
		for k, v := range offsets {
			start := int(v.(float64))
			for offset := start; ; offset++ {
				val, err := kv.ReadInt(context.Background(), fmt.Sprintf("%d_%s", offset, k))
				if err != nil {
					rpcErr := err.(*maelstrom.RPCError)
					if rpcErr.Code == maelstrom.KeyDoesNotExist {
						break
					} else {
						return err
					}
				}
				msgs[k] = append(msgs[k], [2]int{offset, val})
			}
		}
		replyBody := map[string]any{"type": "poll_ok", "msgs": msgs}
		return node.Reply(msg, replyBody)
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]any)
		for k, v := range offsets {
			if err := kv.Write(context.Background(), "c"+k, v); err != nil {
				return err
			}
		}
		return node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		keys := body["keys"].([]any)
		offsets := make(map[string]int)
		for _, k := range keys {
			key := k.(string)
			c, err := kv.ReadInt(context.Background(), "c"+key)
			if err != nil {
				rpcErr := err.(* maelstrom.RPCError)
				if rpcErr.Code == maelstrom.KeyDoesNotExist {
					continue 
				} else {
					return err 
				}
			}
			offsets[key] = c
		}
		return node.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
