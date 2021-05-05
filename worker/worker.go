package worker

import (
	"sync"

	"github.com/ifaisalalam/ide-task-consumer/queue"
)

type Worker struct {
	q           queue.Q
	parallelism int
	wg          *sync.WaitGroup
}

type Handler interface {
	Handle(*sync.WaitGroup, queue.Data)
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
		return err
	}

	for i := 0; i < w.parallelism; i++ {
		w.wg.Add(1)
		go handler.Handle(w.wg, data)
	}

	return nil
}
