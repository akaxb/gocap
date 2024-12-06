package core

import "github.com/akaxb/gocap/model"

type ITransport interface {
	Send(name string, message *model.Message) error //send message to rabbitmq
	Close() error
}

type ICapPublish interface {
	Publish(name string, message *model.Message) error // Publish message to channel
}

type IDispatch interface {
	EnqueueToPublish(message *model.Message)
	Start()
	Stop() error
}
