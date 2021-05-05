package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ifaisalalam/ide-task-consumer/handler"
	"github.com/ifaisalalam/ide-task-consumer/queue"
	"github.com/ifaisalalam/ide-task-consumer/queue/rabbitmq"
	"github.com/ifaisalalam/ide-task-consumer/worker"
)

func main() {
	closing := make(chan bool, 2)
	sig := make(chan os.Signal, 1)
	defer close(closing)
	defer close(sig)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sig
		closing <- true
	}()

	parallelism := 5

	shouldBeQueue, err := queue.Initialize(&rabbitmq.RabbitMQ{PrefetchCount: parallelism})
	if err != nil {
		log.Fatalln(err.Error())
	}
	q, ok := shouldBeQueue.(queue.Q)
	if !ok {
		log.Fatalln("")
	}
	defer q.Close()

	h := &handler.Handler{}
	w := worker.NewWorker(q, parallelism)
	if err = w.Spawn(h); err != nil {
		log.Println("")
		closing <- true
	}

	go func(q queue.Q) {
		<-closing
		_ = q.CloseChannel()
	}(q)

	w.Wait()
}
