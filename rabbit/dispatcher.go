package rabbit

import (
	"context"
	"github.com/akaxb/gocap/core"
	"github.com/akaxb/gocap/model"
	"github.com/akaxb/gocap/storage"
	"log"
	"sync/atomic"
	"time"
)

const defaultEnqueueTimeoutMillisecond = 500

var _ core.IDispatch = (*Dispatcher)(nil)

type Dispatcher struct {
	msgCh       chan *model.MediumMessage
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
		d.msgCh = make(chan *model.MediumMessage, len)
		d.len = len
	}
}

func (d *Dispatcher) EnqueueToPublish(msg *model.MediumMessage) {
	d.logger.Printf("try to send message id:%d to channel", msg.Id)
	ctx, cancel := context.WithTimeout(context.Background(), defaultEnqueueTimeoutMillisecond*time.Millisecond)
	defer cancel()
	//在队列阻塞的时候，尝试重试，直到超时，退出
loop:
	for {
		select {
		case d.msgCh <- msg:
			d.logger.Printf("send message id:%d to channel success", msg.Id)
			break loop
		case <-ctx.Done():
			d.logger.Printf("try send to channel timeout: %v", ctx.Err()) // 记录超时错误
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
			d.logger.Printf("send message id:%d to MQ", msg.Id)
			err := d.transport.Send(msg.Name, msg)
			if err != nil {
				d.logger.Printf("send message to MQ failed %s", err)
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

func (d *Dispatcher) setSuccessfulState(msg *model.MediumMessage) error {
	return d.s.ChangeState(d.initializer.GetPublishedTableName(), msg, model.Success)
}

func (d *Dispatcher) SetFailedState(msg *model.MediumMessage) error {
	return d.s.ChangeState(d.initializer.GetPublishedTableName(), msg, model.Fail)
}
