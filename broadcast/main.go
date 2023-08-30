package main

import (
	"encoding/json"
	"log"
	"strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	messages map[int]struct{}
}

func main() {
	state := State{
		messages: make(map[int]struct{}, 100),
	}

	node := maelstrom.NewNode()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		m := int(body["message"].(float64))
		if _, ok := state.messages[m]; ok {
			return nil
		}
		state.messages[m] = struct{}{}

		if strings.HasPrefix(msg.Src, "n") {
			return nil
		}

		for _, n := range node.NodeIDs() {
			if n == node.ID() {
				continue
			}
			sendBody := map[string]any{"type": "broadcast", "message": m}
			if err := node.Send(n, sendBody); err != nil {
				return err
			}
		}

		return node.Reply(msg, maelstrom.MessageBody{Type: "broadcast_ok"})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["messages"] = state.getMessages()
		body["type"] = "read_ok"
		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		return node.Reply(msg, maelstrom.MessageBody{Type: "topology_ok"})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *State) getMessages() []int {
	messages := make([]int, 0, len(s.messages))
	for k := range s.messages {
		messages = append(messages, k)
	}
	return messages
}
