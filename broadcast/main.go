package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

type Request struct {
	node string
	msg  int
}

func main() {
	neighbors := make([]string, 0, 5)
	state := make(map[int]struct{})
	retry := make(chan Request)
	node := maelstrom.NewNode()

	go func() {
		for {
			req := <-retry
			for {
				body := map[string]any{"type": "gossip", "message": req.msg}
				if err := node.Send(req.node, body); err == nil {
					break
				}
			}
		}
	}()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		respBody := map[string]any{"type": "broadcast_ok"}
		m := int(body["message"].(float64))
		if _, ok := state[m]; ok {
			return node.Reply(msg, respBody)
		}
		state[m] = struct{}{}
		for _, n := range neighbors {
			gossipBody := map[string]any{"type": "gossip", "message": m}
			if err := node.Send(n, gossipBody); err != nil {
				retry <- Request{node: n, msg: m}
				// return err
			}
		}
		return node.Reply(msg, respBody)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		messages := make([]int, 0, len(state))
		for m := range state {
			messages = append(messages, m)
		}
		body := map[string]any{"type": "read_ok", "messages": messages}
		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topology := body["topology"].(map[string]any)
		nodes := topology[node.ID()].([]any)
		for _, n := range nodes {
			neighbors = append(neighbors, n.(string))
		}
		return node.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	node.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		m := int(body["message"].(float64))
		if _, ok := state[m]; ok {
			return nil
		}
		state[m] = struct{}{}
		for _, n := range neighbors {
			if n == msg.Src {
				continue
			}
			gossipBody := map[string]any{"type": "gossip", "message": m}
			if err := node.Send(n, gossipBody); err != nil {
				retry <- Request{node: n, msg: m}
				// return err
			}
		}
		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
