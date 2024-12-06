package rabbit

import (
	"context"
	"fmt"
	"github.com/akaxb/gocap/core"
	"github.com/akaxb/gocap/model"
	"github.com/akaxb/gocap/storage"
	"log"
	"sync/atomic"
	"time"
)

const defaultEnqueueTimeoutMillisecond = 1000

type Dispatcher struct {
	msgCh       chan *model.Message
	len         int
	logger      *log.Logger
	transport   core.ITransport
	closed      int32
	exitChan    chan struct{}
	s           storage.IDataStorage
	initializer storage.IStorageInitializer
}

func NewDispatcher(transport core.ITransport, logger *log.Logger, s storage.IDataStorage, initializer storage.IStorageInitializer, opts ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		transport:   transport,
		logger:      logger,
		s:           s,
		initializer: initializer,
		exitChan:    make(chan struct{}),
	}
	for _, option := range opts {
		option(d)
	}
	return d
}

type DispatcherOption func(*Dispatcher)

func WithQueueLen(len int) DispatcherOption {
	return func(d *Dispatcher) {
		d.msgCh = make(chan *model.Message, len)
		d.len = len
	}
}

func (d *Dispatcher) EnqueueToPublish(msg *model.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultEnqueueTimeoutMillisecond)
	defer cancel()
	//在队列阻塞的时候，尝试重试，直到超时，退出
loop:
	for {
		select {
		case d.msgCh <- msg:
			d.logger.Printf("send message id:%s to channel success", msg.Id)
			break loop
		case <-ctx.Done():
			fmt.Errorf("try send to channel timeout %w", ctx.Err())
			break loop
		default:
			d.logger.Println("channel is full, wait for a while")
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func (d *Dispatcher) Start() {
	d.logger.Println("start sending msg from channel to rabbitmq...")
	d.sending()
}

func (d *Dispatcher) sending() {
	go func() {
		for msg := range d.msgCh {
			d.logger.Printf("send message id:%s to MQ", msg.Id)
			err := d.transport.Send(msg.Name, msg)
			if err != nil {
				d.logger.Printf("send message to mq failed %s", err)

			} else {
				d.setSuccessfulState(msg)
			}
		}
		d.exitChan <- struct{}{}
		d.logger.Println("stop msg from channel to rabbitmq...")
	}()
}

func (d *Dispatcher) Stop() error {
	d.logger.Println("stopping dispatcher ... ")
	if atomic.CompareAndSwapInt32(&d.closed, 0, 1) {
		d.logger.Println("closing msg channel...")
		close(d.msgCh)
		d.logger.Println("msg channel closed successfully")
	}
	//wait for all the messages to be sent
	<-d.exitChan
	d.logger.Println("dispatcher stop successfully")
	return nil
}

func (d *Dispatcher) setSuccessfulState(msg *model.Message) error {
	return d.s.ChangeState(d.initializer.GetPublishedTableName(), msg, model.Success)
}

func (d *Dispatcher) SetFailedState(msg *model.Message) error {
	return d.s.ChangeState(d.initializer.GetPublishedTableName(), msg, model.Fail)
}
