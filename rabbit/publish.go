package rabbit

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/akaxb/gocap/core"
	"github.com/akaxb/gocap/model"
	"github.com/akaxb/gocap/storage"
	"log"
	"reflect"
	"strconv"
	"sync"
	"time"
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
	added := time.Now()
	mm := make(map[string]string)
	mm["cap-msg-name"] = name
	mm["cap-msg-type"] = reflect.TypeOf(message.Value).Name()
	mm["cap-senttime"] = added.String()
	mm["cap-msg-id"] = strconv.FormatInt(message.Id, 10)
	message.Headers = mm
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message data: %w", err)
	}
	mediumMsg := &model.MediumMessage{
		Added:   time.Now(),
		Id:      message.Id,
		Name:    name,
		Content: string(data),
		Origin:  *message,
	}
	tx := p.s.GetTX(message.Id)
	if tx != nil {
		err := p.s.StoreMessageWithTransaction(mediumMsg)
		if err != nil {
			return fmt.Errorf("failed to store message with transaction: %w", err)
		}
	} else {
		err := p.s.StoreMessage(mediumMsg)
		if err != nil {
			return fmt.Errorf("failed to store message: %w", err)
		}
	}
	p.IDispatch.EnqueueToPublish(mediumMsg)
	return nil
}
