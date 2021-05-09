package handler

import (
	"fmt"
	"sync"

	"github.com/ifaisalalam/ide-task-consumer/queue"

	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
)

type Handler struct {
}

func (h *Handler) Handle(id int, wg *sync.WaitGroup, msg queue.Data) {
	defer wg.Done()
	for {
		data := msg.GetData()
		if msg.IsNil(data) == true {
			log.Infoln(fmt.Sprintf("stopping handler %d", id))
			return
		}
		message, err := msg.GetMessage(data)
		if err != nil {
			log.WithError(errors.AddStack(err)).Errorln("failed getting message from the data")
			continue
		}
		fmt.Println(fmt.Sprintf("New message: %s", message))

		err = msg.Ack(data)
		if err != nil {
			log.WithError(errors.AddStack(err)).Errorln("failed ack-ing the message")
		}
	}
}
