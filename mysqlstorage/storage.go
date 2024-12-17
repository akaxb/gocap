package mysqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/akaxb/gocap/model"
	"github.com/akaxb/gocap/storage"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"sync"
	"time"
)

var _ storage.IDataStorage = (*MysqlStorage)(nil)

type MysqlStorage struct {
	log         *log.Logger
	db          *sql.DB
	initializer storage.IStorageInitializer
	mtx         sync.RWMutex
	txMap       map[int64]*sql.Tx
}

func (s *MysqlStorage) SetTX(snowId int64, tx *sql.Tx) {
	//TODO implement me
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.txMap[snowId] = tx
}

func (s *MysqlStorage) GetTX(snowId int64) *sql.Tx {
	//TODO implement me
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.txMap[snowId]
}

var _ storage.IDataStorage = (*MysqlStorage)(nil)

func New(conStr string, logger *log.Logger, initializer storage.IStorageInitializer, options ...MysqlOption) storage.IDataStorage {
	m := &MysqlStorage{
		log:         logger,
		initializer: initializer,
		txMap:       make(map[int64]*sql.Tx),
	}
	db, err := sql.Open("mysql", conStr)
	if err != nil {
		m.log.Fatalf("Open database error:%v", err)
	}
	// 尝试与数据库建立连接（校验dsn是否正确）
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		m.log.Fatalf("Ping database error:%v", err)
	}
	m.db = db
	for _, option := range options {
		option(m)
	}
	return m
}

type MysqlOption func(*MysqlStorage)

func WithMaxOpenConnections(value int) MysqlOption {
	return func(m *MysqlStorage) {
		m.db.SetMaxOpenConns(value)
	}
}

func WithMaxIdleConnections(value int) MysqlOption {
	return func(m *MysqlStorage) {
		m.db.SetMaxIdleConns(value)
	}
}

func (s *MysqlStorage) Close() error {
	return s.db.Close()
}

func (s *MysqlStorage) StoreMessage(mediumMsg *model.MediumMessage) error {
	sql := fmt.Sprintf("INSERT INTO `%s` (`Id`, `Version`, `Name`, `Content`, `Retries`, `Added`, `ExpiresAt`, `StatusName`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", s.initializer.GetPublishedTableName())
	stmt, err := s.db.Prepare(sql)
	if err != nil {
		return fmt.Errorf("StoreMessage：Prepare statement error: %w", err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(mediumMsg.Id, "v1", mediumMsg.Name, mediumMsg.Content, mediumMsg.Retries, time.Now(), nil, model.Scheduled)
	if err != nil {
		return fmt.Errorf("StoreMessage：Exec statement error: %w", err)
	}
	s.log.Printf("StoreMessage: %v", mediumMsg.Id)
	return nil
}

func (s *MysqlStorage) StoreMessageWithTransaction(mediumMsg *model.MediumMessage) error {
	sql1 := fmt.Sprintf("INSERT INTO `%s` (`Id`, `Version`, `Name`, `Content`, `Retries`, `Added`, `ExpiresAt`, `StatusName`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", s.initializer.GetPublishedTableName())
	tx := s.GetTX(mediumMsg.Id)
	//_, err = tx.Exec(sql1, "1867128159846010880", "v1", message.Name, string(data), message.Retries, time.Now(), nil, model.Scheduled)
	_, err := tx.Exec(sql1, mediumMsg.Id, "v1", mediumMsg.Name, mediumMsg.Content, mediumMsg.Retries, time.Now(), nil, model.Scheduled)
	if err != nil {
		s.log.Printf("rollback trans with msg %d", mediumMsg.Id)
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("StoreMessage: Exec and Rollback errors: %w, rollback error: %v", err, rollbackErr)
		}
		return fmt.Errorf("StoreMessage: Exec statement error: %w", err)
	}
	s.removeTransaction(mediumMsg.Id)
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("StoreMessage：Commit error: %w", err)
	}
	return nil
}

func (s *MysqlStorage) ChangeState(tableName string, msg *model.MediumMessage, status model.MessageStatus) error {
	sql := fmt.Sprintf("UPDATE `%s` SET `StatusName` = ?  WHERE `Id` = ?", tableName)
	// Prepare statement for insertion
	stmt, err := s.db.Prepare(sql)
	if err != nil {
		return fmt.Errorf("ChangePublishState：Prepare statement error: %w", err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(status, msg.Id)
	if err != nil {
		return fmt.Errorf("ChangePublishState：Exec statement error: %w", err)
	}
	s.log.Printf("ChangePublishState: %v", msg.Id)
	return nil
}

func (s *MysqlStorage) BeginTransaction(msgId int64) *sql.Tx {
	tx, err := s.db.Begin()
	if err != nil {
		s.log.Fatalf("BeginTransaction error: %v", err)
	}
	s.SetTX(msgId, tx)
	return tx
}

func (s *MysqlStorage) removeTransaction(msgId int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	delete(s.txMap, msgId)
}
