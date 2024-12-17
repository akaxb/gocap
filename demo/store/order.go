package store

import (
	"context"
	"database/sql"
	"errors"
	"github.com/akaxb/gocap/demo/model"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"time"
)

type OrderStorage struct {
	db *sql.DB
}

func NewOrderStorage(conStr string) *OrderStorage {
	m := &OrderStorage{}
	db, err := sql.Open("mysql", conStr)
	if err != nil {
		log.Fatalf("Open database error:%v", err)
	}
	// 尝试与数据库建立连接（校验dsn是否正确）
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatalf("Ping database error:%v", err)
	}
	m.db = db
	return m
}

func (s *OrderStorage) Add(order *model.Orders) error {
	sql := "insert into Orders(id,name) values(?,?)"
	_, err := s.db.Exec(sql, order.Id, order.Name)
	return err
}

func (s *OrderStorage) AddWithTransaction(tx *sql.Tx, order *model.Orders) error {
	sql := "insert into Orders(id,name) values(?,?)"
	_, err := tx.Exec(sql, order.Id, order.Name)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Printf("insert order : unable to rollback: %v", rollbackErr)
		}
		return errors.New("insert order error")
	}
	return nil
}
