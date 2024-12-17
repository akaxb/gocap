package storage

import (
	"context"
	"database/sql"
	"github.com/akaxb/gocap/model"
)

type IDataStorage interface {
	Close() error
	StoreMessage(mediumMsg *model.MediumMessage) error
	StoreMessageWithTransaction(mediumMsg *model.MediumMessage) error
	ChangeState(tableName string, msg *model.MediumMessage, status model.MessageStatus) error
	BeginTransaction(msgId int64) *sql.Tx
	SetTX(snowId int64, tx *sql.Tx)
	GetTX(snowId int64) *sql.Tx
}

type IStorageInitializer interface {
	Initialize(ctx context.Context)
	GetPublishedTableName() string
	GetReceivedTableName() string
}
