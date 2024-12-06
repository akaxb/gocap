package rabbit

import (
	"fmt"
	"github.com/akaxb/gocap/core"
	"github.com/akaxb/gocap/model"
	"github.com/akaxb/gocap/storage"
	"log"
)

type RabbitmqPublish struct {
	logger *log.Logger
	core.IDispatch
	s storage.IDataStorage
}

func NewRabbitmqPublish(logger *log.Logger, dispatcher core.IDispatch, s storage.IDataStorage) *RabbitmqPublish {
	return &RabbitmqPublish{
		logger:    logger,
		IDispatch: dispatcher,
		s:         s,
	}
}

func (p *RabbitmqPublish) Publish(name string, message *model.Message) error {
	err := p.s.StoreMessage(name, message)
	if err != nil {
		return fmt.Errorf("failed to store message: %w", err)
	}
	p.IDispatch.EnqueueToPublish(message)
	return nil
}
