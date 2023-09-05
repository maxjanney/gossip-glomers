package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	// todo: periodically broadcast our count to neighbors?
	// does this work? if so, does this also work with net. partitions?
	node := maelstrom.NewNode()
	neighbors := make([]string, 0, 5)
	kv := maelstrom.NewSeqKV(node)
	done := make(chan struct{})
	ticker := time.NewTicker(500 * time.Millisecond)

	go func() {
		for {
			select {
			case <-done:
				ticker.Stop()
				return 
			case <-ticker.C:
				for _, n := range neighbors {
					count, _ := kv.Read(context.TODO(), node.ID())
					body := map[string]any{"type": "gossip", "count": uint64(count.(int))}
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
			neighbors = append(neighbors, n)
		}
		return kv.Write(context.TODO(), node.ID(), 0)
	})

	node.Handle("add", func(msg maelstrom.Message) error {
		node.Reply(msg, map[string]any{"type": "add_ok"})
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delta := uint64(body["delta"].(float64))
		if delta == 0 {
			return nil
		}
		count, _ := kv.Read(context.TODO(), node.ID())
		return kv.Write(context.TODO(), node.ID(), uint64(count.(int)) + delta)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		count, _ := kv.Read(context.TODO(), node.ID())
		return node.Reply(msg, map[string]any{"type": "read_ok", "value": uint64(count.(int))})
	})

	node.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		count := uint64(body["count"].(float64))
		ours, _ := kv.Read(context.TODO(), node.ID())
		kv.Write(context.TODO(), node.ID(), count + uint64(ours.(int)))
		return nil 
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
		done<-struct{}{}
	}
}