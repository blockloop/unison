package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"bitbucket.org/justbrettjones/unison/q"
	"github.com/streadway/amqp"
)

type Change struct {
	Source   string
	Path     string
	ModDate  time.Time
	IsDelete bool
	IsMod    bool
	IsCreate bool
	IsMove   bool
	Checksum string
}

type Transfer struct {
	Checksum string
	Path     string
	Delivery *amqp.Delivery
}

func RequestFile(change *Change) {
	log.Println("requesting file", change.Path)
	var err error

	connection := <-q.Connection()

	channel, err := connection.Channel()
	if err != nil {
		log.Println(fmt.Errorf("could not open a channel to request a file: %s", err))
		return
	}

	if err := channel.ExchangeDeclare(
		"files",  // name of the exchange
		"direct", // type
		true,     // durable
		true,     // delete when complete
		false,    // internal
		false,    // noWait
		nil,      // arguments
	); err != nil {
		panic(fmt.Errorf("Exchange Declare: %s", err))
	}

	queue, err := channel.QueueDeclare(
		fmt.Sprintf("%s:%s", hostname, change.Path), // name of the queue
		false,        // durable
		true,         // delete when usused
		false,        // exclusive
		false,        // noWait
		amqp.Table{}, // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Queue Declare: %s", err))
	}

	if err = channel.QueueBind(
		queue.Name,  // name of the queue
		change.Path, // bindingKey
		"files",     // sourceExchange
		false,       // noWait
		nil,         // arguments
	); err != nil {
		panic(fmt.Errorf("Queue Bind: %s", err))
	}

	t := &Transfer{
		Checksum: change.Checksum,
		Path:     change.Path,
	}
	req, _ := json.Marshal(t)

	err = channel.Publish(
		"file-requests", // exchange
		"",              // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        req,
		})

	go WaitForFile(channel, change.Path, queue.Name)
}

func WaitForFile(channel *amqp.Channel, filePath string, queueName string) {
	msgs, err := channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		panic(fmt.Errorf("Failed to register a consumer: %s", err.Error()))
	}
	log.Println("waiting for file to be transfered on queue:", filePath)

	go func() {
		defer channel.Close()
		for {
			d := <-msgs
			log.Printf("received binary blob %d for %s\nBLOB: %s", d.Headers, filePath, string(d.Body))
		}
	}()
}
