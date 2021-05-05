package rabbitmq

import (
	"errors"
	"fmt"

	"github.com/ifaisalalam/ide-task-consumer/queue"

	"github.com/streadway/amqp"
)

const (
	QueueProvider = "RabbitMQ"
	consumerName  = "ide-task-consumer"
)

var (
	ErrNilMessageData     = errors.New(fmt.Sprintf("%s: message Data is nil", QueueProvider))
	ErrInvalidMessageData = errors.New(fmt.Sprintf("%s: failed to cast message data to amqp.Delivery", QueueProvider))
)

type RabbitMQ struct {
	conn          *amqp.Connection
	q             amqp.Queue
	ch            []*amqp.Channel
	PrefetchCount int
}

func (r *RabbitMQ) Connect() (interface{}, error) {
	amqpConfig := amqp.Config{}
	var err error
	r.conn, err = amqp.DialConfig("amqp://localhost:5672", amqpConfig)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to dial AMQP. Error: %s", err.Error()))
	}

	ch, err := r.conn.Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close()
	r.q, err = ch.QueueDeclare("test", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	return r.conn, nil
}

func (r *RabbitMQ) Close() error {
	for i := 0; i < len(r.ch); i++ {
		if r.ch[i] != nil {
			_ = r.ch[i].Close()
		}
	}
	if r.conn != nil {
		return r.conn.Close()
	}

	return nil
}

func (r *RabbitMQ) CloseChannel() error {
	var err error
	for i := 0; i < len(r.ch); i++ {
		if r.ch[i] != nil {
			err = r.ch[i].Cancel(consumerName, false)
		}
	}

	return err
}

func (r *RabbitMQ) GetChannel() (queue.Data, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return queue.Data{}, err
	}
	if err = ch.Qos(r.PrefetchCount, 0, false); err != nil {
		return queue.Data{}, err
	}
	r.ch = append(r.ch, ch)
	delivery, err := ch.Consume(r.q.Name,
		consumerName,
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		return queue.Data{}, err
	}

	return queue.Data{MessageData: MessageData{delivery}}, nil
}

type MessageData struct {
	data <-chan amqp.Delivery
}

func (md MessageData) GetData() interface{} {
	return <-md.data
}

func (md MessageData) IsNil(data interface{}) bool {
	if data == nil {
		return true
	}

	d, ok := data.(amqp.Delivery)
	if !ok {
		return true
	}
	return d.ConsumerTag == ""
}

func (md MessageData) GetMessage(data interface{}) (string, error) {
	if md.IsNil(data) {
		return "", ErrNilMessageData
	}

	d, ok := data.(amqp.Delivery)
	if !ok {
		return "", ErrInvalidMessageData
	}
	return string(d.Body), nil
}

func (md MessageData) Ack(data interface{}) error {
	if md.IsNil(data) {
		return ErrNilMessageData
	}

	d, ok := data.(amqp.Delivery)
	if !ok {
		return ErrInvalidMessageData
	}
	return d.Ack(false)
}

func (md MessageData) Nack(data interface{}) error {
	if md.IsNil(data) {
		return ErrNilMessageData
	}

	d, ok := data.(amqp.Delivery)
	if !ok {
		return ErrInvalidMessageData
	}
	return d.Nack(false, true)
}
