package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ifaisalalam/ide-task-consumer/env"
	"github.com/ifaisalalam/ide-task-consumer/handler"
	"github.com/ifaisalalam/ide-task-consumer/logger"
	"github.com/ifaisalalam/ide-task-consumer/queue"
	"github.com/ifaisalalam/ide-task-consumer/queue/rabbitmq"
	"github.com/ifaisalalam/ide-task-consumer/worker"

	"github.com/getsentry/sentry-go"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func main() {
	closing := make(chan struct{}, 2)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer close(closing)
	defer func() {
		close(sig)
		time.Sleep(1 * time.Second)
	}()

	viper.AutomaticEnv()
	env.InitEnv()

	log.SetFormatter(&log.JSONFormatter{})
	if env.AppEnv == env.Local {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	err := sentry.Init(sentry.ClientOptions{})
	if err != nil {
		log.WithError(errors.AddStack(err)).Errorln("failed to initialize sentry")
		return
	}
	// Flush buffered events before the program terminates.
	defer sentry.Flush(2 * time.Second)
	log.AddHook(logger.NewSentryHook())

	parallelism := viper.GetInt("IDE_TASK_PARALLELISM")
	if parallelism == 0 {
		parallelism = 3
	}

	shouldBeQueue, err := queue.Initialize(&rabbitmq.RabbitMQ{PrefetchCount: parallelism})
	q, ok := shouldBeQueue.(queue.Q)
	if err != nil || !ok {
		log.WithError(errors.AddStack(err)).WithField("q", q).Errorln("failed to initialize queue")
		return
	}
	defer q.Close()

	h := &handler.Handler{}
	w := worker.NewWorker(q, parallelism)
	if err = w.Spawn(h); err != nil {
		log.WithError(errors.AddStack(err)).Errorln("failed to spawn a worker")
		closing <- struct{}{}
	}

	go func(q queue.Q) {
		<-closing
		_ = q.CloseChannel()
	}(q)
	go func() {
		<-sig
		closing <- struct{}{}
	}()

	w.Wait()
}
