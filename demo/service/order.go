package service

import (
	"database/sql"
	"github.com/akaxb/gocap/core"
	"github.com/akaxb/gocap/demo/model"
	"github.com/akaxb/gocap/demo/store"
)

type OrderService struct {
	OrderStorage *store.OrderStorage
	publisher    core.ICapPublish
}

func (u *OrderService) Add(order *model.Order) {
	u.OrderStorage.Add(order)
}

func (u *OrderService) AddWithTransaction(tx *sql.Tx, order *model.Order) error {
	return u.OrderStorage.AddWithTransaction(tx, order)
}
