package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/akaxb/gocap/core"
	model2 "github.com/akaxb/gocap/demo/model"
	"github.com/akaxb/gocap/demo/service"
	store2 "github.com/akaxb/gocap/demo/store"
	"github.com/akaxb/gocap/model"
	"github.com/akaxb/gocap/mysqlstorage"
	"github.com/akaxb/gocap/rabbit"
	"github.com/akaxb/gocap/storage"
	"github.com/akaxb/gocap/tools"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// multiStatements=true 允许执行多条语句
const (
	dbName   = "capdemo"
	userName = "gaia"
	password = "Gaia@1234"
	host     = "192.168.0.160"
	port     = "3306"
)

var (
	logger        = log.New(os.Stdout, "cap:", log.LstdFlags)
	transport     core.ITransport
	dispatcher    core.IDispatch
	store         storage.IDataStorage
	publisher     core.ICapPublish
	initializer   storage.IStorageInitializer
	connectionStr string
	sf            *tools.Snowflake
	orderStorage  *store2.OrderStorage
	orderService  *service.OrderService
)

func init() {
	sf = tools.NewSnowflake(1)
	connectionStr = mysqlstorage.GenerateConnectionString(userName, password, host, port, dbName)
	logger.Println("tables initializing...")
	initializer = mysqlstorage.NewMysqlInitializer(connectionStr, "cap", logger)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	initializer.Initialize(ctx)
	logger.Println("tables successful initialized...")
}

func main() {
	capOption := model.NewCapOptions(model.WithVersion("v1"))
	store = mysqlstorage.New(
		connectionStr,
		logger,
		initializer,
		mysqlstorage.WithMaxIdleConnections(10),
		mysqlstorage.WithMaxOpenConnections(100),
	)
	transport = rabbit.NewTransport(logger, rabbit.RabbitmqOptions{
		ExchangeName:    "cap-demo-go-exchange",
		ExchangeType:    "topic",
		HostName:        "192.168.0.113",
		Password:        "guest",
		Port:            5672,
		PublishConfirms: false,
		UserName:        "guest",
		VirtualHost:     "/",
		CapOption:       capOption,
	},
	)
	dispatcher = rabbit.NewDispatcher(transport, logger, store, initializer, rabbit.WithQueueLen(10))
	dispatcher.Start()
	publisher = rabbit.NewRabbitmqPublish(logger, dispatcher, store)
	//
	orderStorage = store2.NewOrderStorage(connectionStr)
	orderService = &service.OrderService{
		OrderStorage: orderStorage,
	}
	server := &http.Server{
		Addr: ":8083",
	}
	http.Handle("/publish", http.HandlerFunc(publishHandler))
	http.Handle("/publish/trans", http.HandlerFunc(publishTransHandler))
	// 启动 HTTP 服务器
	go func() {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Println("HTTP server failed: %v\n", err)
		}
	}()
	// 监听系统信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// 等待信号
	<-sigChan
	// 触发关闭逻辑
	logger.Println("Shutting down gracefully...")
	// 设置超时时间
	shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownRelease()
	// 关闭 HTTP 服务器
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("HTTP server shutdown failed: %v\n", err)
	} else {
		logger.Println("HTTP server shutdown successfully")
	}
	if err := stopComponents(); err != nil {
		logger.Printf("a component shutdown failed: %v\n", err)
	} else {
		logger.Println("all Components shutdown successfully")
	}
	time.Sleep(time.Second)
	logger.Println("the application Graceful shutdown complete.")
}

func stopComponents() error {
	//顺序执行
	//先关闭dispatcher channel
	//再关闭transport
	//可以同步关闭数据库
	if err := dispatcher.Stop(); err != nil {
		return err
	}
	if err := transport.Close(); err != nil {
		return err
	}
	if err := store.Close(); err != nil {
		return err
	}
	return nil
}

func publishHandler(resp http.ResponseWriter, req *http.Request) {
	msg := req.URL.Query().Get("msg")
	if msg == "" {
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write([]byte("msg is empty"))
		return
	}
	id := sf.NextID()
	err := publish("test.dino", model.Message{
		Value: msg,
		Id:    id,
	})
	if err != nil {
		logger.Printf("publish failed: %v", err)
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write([]byte("publish failed"))
		return
	}
	resp.Write([]byte("publish success"))
}

func publishTransHandler(resp http.ResponseWriter, req *http.Request) {
	msg := req.URL.Query().Get("msg")
	if msg == "" {
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write([]byte("msg is empty"))
		return
	}
	id := sf.NextID()
	order := &model2.Orders{
		Id:   id,
		Name: msg,
	}
	trans := store.BeginTransaction(id)
	err := orderService.AddWithTransaction(trans, order)
	if err != nil {
		logger.Printf("publish failed: %v", err)
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write([]byte("publish failed"))
		return
	}
	err = publish("dino.xu.new", model.Message{
		Value: order,
		Id:    id,
	})
	if err != nil {
		logger.Printf("publish failed: %v", err)
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write([]byte("publish failed"))
		return
	}
	resp.Write([]byte(fmt.Sprintf("publish success - %d ", id)))
}

func publish(name string, message model.Message) error {
	err := publisher.Publish(name, &model.Message{
		Value: message.Value,
		Id:    message.Id,
	})
	if err != nil {
		return err
	}
	return nil
}
