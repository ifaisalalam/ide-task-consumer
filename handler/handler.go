package handler

import (
	"fmt"
	"sync"

	"github.com/ifaisalalam/ide-task-consumer/queue"
)

type Handler struct {
}

func (h *Handler) Handle(wg *sync.WaitGroup, msg queue.Data) {
	defer wg.Done()
	for {
		data := msg.GetData()
		if msg.IsNil(data) == true {
			fmt.Println("Stopping handler...")
			return
		}
		message, err := msg.GetMessage(data)
		if err != nil {
			// TODO: Log error.
			continue
		}
		fmt.Println(fmt.Sprintf("New message: %s", message))

		err = msg.Ack(data)
		if err != nil {
			fmt.Println(fmt.Sprintf("Ack failed: %s", err.Error()))
		}
	}
}
