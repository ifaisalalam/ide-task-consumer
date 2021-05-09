package queue

import "github.com/pingcap/errors"

type Q struct {
	q Queue
}

type Queue interface {
	Connect() (interface{}, error)
	Close() error
	CloseChannel() error
	GetChannel() (Data, error)
}

type Data struct {
	MessageData
}

type MessageData interface {
	GetData() interface{}
	IsNil(interface{}) bool
	GetMessage(interface{}) (string, error)
	Ack(interface{}) error
	Nack(interface{}) error
}

func Initialize(queue Queue) (interface{}, error) {
	_, err := queue.Connect()
	if err != nil {
		return nil, errors.AddStack(err)
	}

	return Q{queue}, nil
}

func (q Q) Close() error {
	return q.q.Close()
}

func (q Q) CloseChannel() error {
	return q.q.CloseChannel()
}

func (q Q) GetChannel() (Data, error) {
	return q.q.GetChannel()
}

func (data Data) GetData() interface{} {
	return data.MessageData.GetData()
}

func (data Data) IsNil(messageData interface{}) bool {
	return data.MessageData.IsNil(messageData)
}

func (data Data) GetMessage(messageData interface{}) (string, error) {
	return data.MessageData.GetMessage(messageData)
}

func (data Data) Ack(messageData interface{}) error {
	return data.MessageData.Ack(messageData)
}

func (data Data) Nack(messageData interface{}) error {
	return data.MessageData.Nack(messageData)
}
