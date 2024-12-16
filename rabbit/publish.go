package rabbit

import (
	"database/sql"
	"fmt"
	"github.com/akaxb/gocap/core"
	"github.com/akaxb/gocap/model"
	"github.com/akaxb/gocap/storage"
	"log"
	"sync"
)

var _ core.ICapPublish = (*RabbitmqPublish)(nil)

type RabbitmqPublish struct {
	logger *log.Logger
	core.IDispatch
	s     storage.IDataStorage
	txMap map[int64]*sql.Tx
	mtx   sync.RWMutex
}

func NewRabbitmqPublish(logger *log.Logger, dispatcher core.IDispatch, s storage.IDataStorage) *RabbitmqPublish {
	return &RabbitmqPublish{
		logger:    logger,
		IDispatch: dispatcher,
		s:         s,
	}
}

func (p *RabbitmqPublish) Publish(name string, message *model.Message) error {
	message.Name = name
	tx := p.s.GetTX(message.Id)
	if tx != nil {
		err := p.s.StoreMessageWithTransaction(message)
		if err != nil {
			return fmt.Errorf("failed to store message with transaction: %w", err)
		}
	} else {
		err := p.s.StoreMessage(message)
		if err != nil {
			return fmt.Errorf("failed to store message: %w", err)
		}
	}
	p.IDispatch.EnqueueToPublish(message)
	return nil
}
