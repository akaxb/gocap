package mysqlstorage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/akaxb/gocap/model"
	"github.com/akaxb/gocap/storage"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"time"
)

type MysqlStorage struct {
	log         *log.Logger
	db          *sql.DB
	initializer storage.IStorageInitializer
}

func New(conStr string, logger *log.Logger, initializer storage.IStorageInitializer, options ...MysqlOption) storage.IDataStorage {
	m := &MysqlStorage{
		log:         logger,
		initializer: initializer,
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

func (s *MysqlStorage) StoreMessage(name string, message *model.Message) error {
	sql := fmt.Sprintf("INSERT INTO `%s` (`Id`, `Version`, `Name`, `Content`, `Retries`, `Added`, `ExpiresAt`, `StatusName`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", s.initializer.GetPublishedTableName())
	//s.log.Printf("StoreMessage:the sql is %s", sql)
	data, err := json.Marshal(message.Data)
	if err != nil {
		return fmt.Errorf("StoreMessage：json marshal error: %w", err)
	}
	stmt, err := s.db.Prepare(sql)
	if err != nil {
		return fmt.Errorf("StoreMessage：Prepare statement error: %w", err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(message.Id, "v1", message.Name, string(data), message.Retries, time.Now(), nil, model.Scheduled)
	if err != nil {
		return fmt.Errorf("StoreMessage：Exec statement error: %w", err)
	}
	s.log.Printf("StoreMessage: %v", message.Id)
	return nil
}

func (s *MysqlStorage) ChangeState(tableName string, msg *model.Message, status model.MessageStatus) error {
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
