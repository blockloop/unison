package q

import (
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

var (
	// amqpURI = "amqp://***REMOVED***@owl.rmq.cloudamqp.com/***REMOVED***" // "AMQP URI"
	amqpURI = "amqp://brett:***REMOVED***@jane" // "AMQP URI"
	con     *amqp.Connection
	mutex   *sync.Mutex
)

func init() {
	mutex = &sync.Mutex{}
}

func Connection() (ch chan *amqp.Connection) {
	mutex.Lock()
	ch = make(chan *amqp.Connection, 1)
	go func() {
		defer mutex.Unlock()
		if con != nil {
			ch <- con
			return
		}

		con, err := amqp.Dial(amqpURI)
		if err != nil {
			panic(fmt.Errorf("Dial: %s", err))
		}
		ch <- con
	}()

	return ch
}
