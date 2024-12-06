package rabbit

import (
	"fmt"
	"github.com/akaxb/gocap/model"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
	"sync/atomic"
)

const defaultPoolSize = 15

type ConnectionChannelPool struct {
	logger            *log.Logger
	maxSize           int
	pool              chan *amqp.Channel
	connection        *amqp.Connection
	count             int32
	isPublishConfirms bool
	hostAddress       string
	mtx               sync.Mutex
	rabbitmqOptions   RabbitmqOptions
	exchange          string
}

type RabbitmqOptions struct {
	HostName        string
	Port            int
	UserName        string
	Password        string
	VirtualHost     string
	ExchangeName    string
	ExchangeType    string
	PublishConfirms bool
	CapOption       *model.CapOptions
	//ConnectionFactoryOptions func(*amqp.ConnectionConfig)
}

func NewConnectionChannelPool(logger *log.Logger, rabbitmqOptions RabbitmqOptions) *ConnectionChannelPool {
	pool := make(chan *amqp.Channel, defaultPoolSize)
	maxSize := defaultPoolSize
	hostAddress := fmt.Sprintf("%s:%d", rabbitmqOptions.HostName, rabbitmqOptions.Port)
	var exchange string
	capOptions := rabbitmqOptions.CapOption
	if capOptions.Version == "v1" {
		exchange = rabbitmqOptions.ExchangeName
	} else {
		exchange = fmt.Sprintf("%s.%s", rabbitmqOptions.ExchangeName, capOptions.Version)
	}
	logger.Printf("RabbitMQ configuration: 'HostName:%s, Port:%d, UserName:%s, Password:%s, ExchangeName:%s'",
		rabbitmqOptions.HostName, rabbitmqOptions.Port, rabbitmqOptions.UserName, rabbitmqOptions.Password, rabbitmqOptions.ExchangeName)
	return &ConnectionChannelPool{
		logger:            logger,
		maxSize:           maxSize,
		pool:              pool,
		isPublishConfirms: rabbitmqOptions.PublishConfirms,
		hostAddress:       hostAddress,
		rabbitmqOptions:   rabbitmqOptions,
		exchange:          exchange,
	}
}

func (p *ConnectionChannelPool) Rent() (*amqp.Channel, error) {
	select {
	case ch := <-p.pool:
		atomic.AddInt32(&p.count, -1)
		return ch, nil
	default:
	}
	conn, err := p.GetConnection()
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	err = ch.ExchangeDeclare(p.rabbitmqOptions.ExchangeName, p.rabbitmqOptions.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	if p.isPublishConfirms {
		if err := ch.Confirm(false); err != nil {
			return nil, err
		}
	}
	return ch, nil
}

func (p *ConnectionChannelPool) Return(ch *amqp.Channel) error {
	if atomic.AddInt32(&p.count, 1) <= int32(p.maxSize) && (!ch.IsClosed()) {
		p.pool <- ch
		return nil
	}
	if err := ch.Close(); err != nil {
		return err
	}
	atomic.AddInt32(&p.count, -1)
	return nil
}

func (p *ConnectionChannelPool) GetConnection() (*amqp.Connection, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if p.connection != nil && (!p.connection.IsClosed()) {
		return p.connection, nil
	}
	conn, err := amqp.Dial(p.getConnectionURL())
	if err != nil {
		return nil, err
	}
	p.connection = conn
	return conn, nil
}

func (p *ConnectionChannelPool) getConnectionURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", p.rabbitmqOptions.UserName, p.rabbitmqOptions.Password, p.rabbitmqOptions.HostName, p.rabbitmqOptions.Port, p.rabbitmqOptions.VirtualHost)
}

func (p *ConnectionChannelPool) Dispose() {
	p.maxSize = 0
	close(p.pool)
	for ch := range p.pool {
		if err := ch.Close(); err != nil {
			p.logger.Println("Failed to close channel:", err)
		}
	}
	if p.connection != nil && (!p.connection.IsClosed()) {
		if err := p.connection.Close(); err != nil {
			p.logger.Println("Failed to close connection:", err)
		}
	}
}
