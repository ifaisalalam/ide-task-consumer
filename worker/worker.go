package worker

import (
	"sync"

	"github.com/ifaisalalam/ide-task-consumer/queue"

	"github.com/pingcap/errors"
)

type Worker struct {
	q           queue.Q
	parallelism int
	wg          *sync.WaitGroup
}

type Handler interface {
	Handle(id int, wg *sync.WaitGroup, msg queue.Data)
}

func NewWorker(q queue.Q, parallelism int) Worker {
	var wg sync.WaitGroup
	return Worker{q, parallelism, &wg}
}

func (w *Worker) Wait() {
	w.wg.Wait()
}

func (w *Worker) Spawn(handler Handler) error {
	data, err := w.q.GetChannel()
	if err != nil {
		return errors.AddStack(err)
	}

	for i := 0; i < w.parallelism; i++ {
		w.wg.Add(1)
		go handler.Handle(i, w.wg, data)
	}

	return nil
}
