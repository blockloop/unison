package q

import (
	"flag"
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

var (
	amqpHost = flag.String("AMQPHost", "localhost", "RabbitMQ Host")
	con      *amqp.Connection

	mu = &sync.Mutex{}
)

func init() {
	flag.Parse()
}

func Connection() (ch chan *amqp.Connection) {
	ch = make(chan *amqp.Connection, 1)
	go func() {
		mu.Lock()
		defer mu.Unlock()

		if con != nil {
			ch <- con
			return
		}

		uri := fmt.Sprintf("amqp://brett:abc123@%s", *amqpHost)
		log.Println("dialing", uri)

		var err error
		con, err = amqp.Dial(uri)

		if err != nil {
			panic(fmt.Errorf("Dial: %s", err))
		}
		ch <- con
	}()
	return ch
}

func MustChan() *amqp.Channel {
	con := <-Connection()
	ch, err := con.Channel()
	if err != nil {
		panic(err)
	}
	return ch
}
