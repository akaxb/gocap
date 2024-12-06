package storage

import (
	"context"
	"github.com/akaxb/gocap/model"
)

type IDataStorage interface {
	Close() error
	StoreMessage(name string, message *model.Message) error
	ChangeState(tableName string, msg *model.Message, status model.MessageStatus) error
}

type IStorageInitializer interface {
	Initialize(ctx context.Context)
	GetPublishedTableName() string
	GetReceivedTableName() string
}
