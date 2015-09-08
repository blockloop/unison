package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"bitbucket.org/justbrettjones/unison/q"
	"github.com/streadway/amqp"
)

var (
	hostname string
	amqpCon  *amqp.Connection
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	amqpCon = <-q.Connection()
	go ListenForChanges(mustGetChan(amqpCon))
	go WatchLocalChanges(mustGetChan(amqpCon))
	go ListenForTransferRequests(mustGetChan(amqpCon))

	select {}
	log.Println("gracefully exiting...")
}

func ListenForChanges(channel *amqp.Channel) {
	var err error
	hostname, _ = os.Hostname()

	log.Println("CONSUMER: got Channel, declaring Exchange")
	if err = channel.ExchangeDeclare(
		"changes", // name of the exchange
		"fanout",  // type
		true,      // durable
		true,      // delete when complete
		false,     // internal
		false,     // noWait
		nil,       // arguments
	); err != nil {
		panic(fmt.Errorf("Exchange Declare: %s", err))
	}

	log.Printf("CONSUMER: declared Exchange, declaring Queue (%s)", hostname)
	state, err := channel.QueueDeclare(
		hostname,     // name of the queue
		true,         // durable
		false,        // delete when usused
		false,        // exclusive
		false,        // noWait
		amqp.Table{}, // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Queue Declare: %s", err))
	}

	log.Printf("CONSUMER: declared Queue (%d messages, %d consumers), binding to Exchange (key '%s')",
		state.Messages, state.Consumers, "")

	if err = channel.QueueBind(
		hostname,  // name of the queue
		"",        // bindingKey
		"changes", // sourceExchange
		false,     // noWait
		nil,       // arguments
	); err != nil {
		panic(fmt.Errorf("Queue Bind: %s", err))
	}

	log.Printf("CONSUMER: Queue bound to Exchange, starting Consume")
	deliveries, err := channel.Consume(
		hostname, // name
		"",       // consumerTag,
		true,     // auto-ack
		false,    // exclusive
		false,    // noLocal
		false,    // noWait
		nil,      // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Queue Consume: %s", err))
	}

	go handle(deliveries)
}

func handle(deliveries <-chan amqp.Delivery) {
	for {
		d := <-deliveries
		d.Ack(false)

		var msg Change
		if err := json.Unmarshal(d.Body, &msg); err != nil {
			log.Printf("Could not unmarshal", string(d.Body))
			return
		}
		if msg.Source == hostname {
			return
		}
		go HandleChange(&msg)
	}
	log.Printf("CONSUMER: handle: deliveries channel closed")
}

func HandleChange(change *Change) {
	if change.IsCreate || change.IsMod {
		go RequestFile(change)
		return
	}

	fullPath := filepath.Join(rootDir, change.Path)
	local, infoErr := os.Stat(fullPath)

	if change.IsDelete || change.IsMove {
		if infoErr != nil {
			log.Println("file appears to be already deleted", change.Path, infoErr.Error())
			return
		}

		if change.ModDate.Before(local.ModTime().UTC()) {
			//TODO conflict??
			log.Println("local conflict for file", change.Path)
			return
		}

		if _, err := os.Stat(fullPath); err == nil {
			if err := os.Remove(fullPath); err != nil {
				log.Println("could not delete local file", fullPath)
			}
		}
	}

}

func mustGetChan(con *amqp.Connection) *amqp.Channel {
	ch, err := con.Channel()
	if err != nil {
		panic(err)
	}
	return ch
}
