package main 

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	// todo: periodically broadcast our count to neighbors?
	// does this work? if so, does this also work with net. partitions?
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	node.Handle("init", func(msg maelstrom.Message) error {
		kv.Write(context.TODO(), node.ID(), 0)
		return nil 
	})

	node.Handle("add", func(msg maelstrom.Message) error {
		node.Reply(msg, map[string]any{"type": "read_ok"})
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delta := int(body["delta"].(float64))
		count, _ := kv.ReadInt(context.TODO(), node.ID())
		kv.Write(context.TODO(), node.ID(), count + delta)
		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		count, _ := kv.ReadInt(context.TODO(), node.ID())
		return node.Reply(msg, map[string]any{"type": "read_ok", "value": count})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}