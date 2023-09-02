package main

import (
	"encoding/json"
	"log"
	"strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	messages map[int]struct{}
	known    map[string]map[int]struct{}
}

// func init() {
// 	f, err := os.OpenFile("/tmp/app.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	log.SetOutput(f)
// 	// f, err := os.OpenFile("logfile", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
// 	// if err != nil {
// 	// 	log.Fatalf("error opening file: %v", err)
// 	// }
// 	// defer f.Close()

// 	// log.SetOutput(f)
// 	// log.Println("This is a test log entry")
// }

func main() {

	state := State{
		messages: make(map[int]struct{}, 150),
		known:    make(map[string]map[int]struct{}),
	}

	node := maelstrom.NewNode()

	node.Handle("init", func(msg maelstrom.Message) error {
		for _, n := range node.NodeIDs() {
			if n == node.ID() {
				continue
			}
			state.known[n] = make(map[int]struct{})
		}
		return node.Reply(msg, map[string]any{"type": "init_ok"})
	})
	// c := make(chan Request)

	// go func(c chan Request) {
	// 	// for i := 0; i < 20; i++ {
	// 	// 	go func(c chan Request) {
	// 	// 		for {
	// 	// 			req := <- c
	// 	// 			body := map[string]any{"type": "broadcast", "message": req.m}
	// 	// 			for {
	// 	// 				if err := node.Send(req.n, body); err == nil {
	// 	// 					break
	// 	// 				}
	// 	// 			}
	// 	// 		}
	// 	// 	}(c)
	// 	// }
	// 	log.Printf("Starting retry routine")
	// 	for {
	// 		req := <-c
	// 		go func(c chan Request) {
	// 			body := map[string]any{"type": "broadcast", "message": req.m}
	// 			if err := node.Send(req.n, body); err != nil {
	// 				// request failed, retry
	// 				log.Print("Failed to send %v to %v\n", req.m, req.n)
	// 				c <- req
	// 			}

	// 			// var respBody map[string]any
	// 			// if err := json.Unmarshal(msg.Body, &respBody); err != nil {
	// 			// 	log.Printf("??: %v\n", err.Error())
	// 			// 	c <- req
	// 			// 	return
	// 			// }

	// 			// if respBody["type"] != "broadcast_ok" {
	// 			// 	log.Printf("Da fuq\n")
	// 			// 	c <- req
	// 			// }
	// 		}(c)
	// 	}
	// }(c)

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		m := int(body["message"].(float64))
		// if state.known[msg.Src] == nil {
		// 	state.known[msg.Src] = make(map[int]struct{})
		// }
		if strings.HasPrefix(msg.Src, "n") {
			state.known[msg.Src][m] = struct{}{}
		}

		if _, ok := state.messages[m]; ok {
			return node.Reply(msg, map[string]any{"type": "broadcast_ok"})
		}
		state.messages[m] = struct{}{}

		for _, n := range node.NodeIDs() {
			if n == node.ID() || n == msg.Src {
				continue 
			}

			if _, ok := state.known[n][m]; ok {
				continue
			}

			body := map[string]any{"type": "broadcast", "message": m}
			if err := node.Send(n, body); err != nil {
				log.Fatal(err)
			}
		}

		// neighbors := make(map[string]struct{})
		// for _, n := range node.NodeIDs() {
		// 	if n != node.ID() {
		// 		if _, ok := state.known[n][m]; !ok {
		// 			neighbors[n] = struct{}{}
		// 		}
		// 	}
		// }

		// for len(neighbors) > 0 {
		// for n := range neighbors {
		// 	if err := node.RPC(n, map[string]any{"type": "broadcast", "message": m}, func(msgg maelstrom.Message) error {
		// 		var bbody map[string]any
		// 		if err := json.Unmarshal(msgg.Body, &bbody); err != nil {
		// 			return err
		// 		}
		// 		if bbody["type"] == "broadcast_ok" {
		// 			delete(neighbors, n)
		// 		}
		// 		return nil
		// 	}); err != nil {
		// 		log.Fatal(err)
		// 	}
		// }
		// }

		// go func(n string, m int) {
		// 	rpcBody := map[string]{"type": "broadcast", "message": m}
		// 	acks := make(map[string]struct{})
		// 	for _, n := range node.IDs() {
		// 		if n != node.ID() {
		// 			acks[]
		// 		}
		// 	}
		// 	for len(acks) > 0 {
		// 		if err := node.RPC(n, rpcBody, func(msg maelstrom.Message) error {
		// 			var body map[string]any
		// 			if err := json.Unmarshal(msg.Body, &body); err != nil {
		// 				return err
		// 			}
		// 			if body["type"] == "broadcast_ok" {
		// 				delete(acks, )
		// 			}
		// 		}); err != nil {
		// 			log.Fatal(err)
		// 		}
		// }(n, m)

		// for _, n := range node.NodeIDs() {
		// 	if n == node.ID() || n == msg.Src {
		// 		continue
		// 	}

		// 	if _, ok := state.known[n][m]; ok {
		// 		continue
		// 	}

		// 	// if err := node.Send(n, map[string]any{"type": "broadcast", "message": m}); err != nil {
		// 	// 	log.Fatal(err)
		// 	// }
		// }

		// if strings.HasPrefix(msg.Src, "n") {
		// 	return nil
		// }

		// neighbors := make(map[string]struct{})
		// for _, n := range node.NodeIDs() {
		// 	if n != node.ID() {
		// 		neighbors[n] = struct{}{}
		// 	}
		// }
		// delete(neighbors, msg.Src)

		// for len(neighbors) > 0 {
		// 	for n := range neighbors {
		// 		node.RPC(n, map[string]any{"type": "broadcast", "message": m}, func(msg maelstrom.Message) error {
		// 			var body map[string]any
		// 			if err := json.Unmarshal(msg.Body, &body); err != nil {
		// 				return err
		// 			}
		// 			if body["type"] == "broadcast_ok" {
		// 				delete(neighbors, n)
		// 			}
		// 			return nil
		// 		})
		// 	}
		// }

		// for _, n := range node.NodeIDs() {
		// 	if n == node.ID() || n == msg.Src {
		// 		continue
		// 	}
		// 	// c <- Request{m: m, n: n}
		// 	go func(n string) {
		// 		body := map[string]any{"type": "broadcast", "message": m}
		// 		for {
		// 			if err := node.RPC(n, body, func(msg maelstrom.Message) error {
		// 				var body map[string]any
		// 				if err := json.Unmarshal(msg.Body, &body); err != nil {
		// 					return err
		// 				}
		// 				if body["type"] == "broadcast_ok" {
		// 					return nil
		// 				}
		// 				return nil
		// 			}); err == nil {
		// 				log.Fatal("Sent %v to %v\n", m, n)
		// 			}
		// 		}
		// 	}(n)
		// }
		return node.Reply(msg, map[string]any{"type": "broadcast_ok"})
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

	node.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
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
