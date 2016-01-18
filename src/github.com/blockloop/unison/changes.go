package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/blockloop/unison/q"

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
	Requestor string
	Checksum  string
	Path      string
}

func ListenForChanges() {
	var err error
	hostname, _ = os.Hostname()

	channel := q.MustChan()

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

	log.Printf("CONSUMER: declaring Queue (%s)", hostname)
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

	log.Printf("CONSUMER: binding to Exchange (key '%s')", state.Messages, state.Consumers, "")

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
	changes, err := channel.Consume(
		hostname, // name
		"",       // consumerTag,
		false,    // auto-ack
		false,    // exclusive
		false,    // noLocal
		false,    // noWait
		nil,      // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Queue Consume: %s", err))
	}

	go handle(changes)
}

func RequestFile(change *Change) {
	log.Println("requesting file", change.Path)
	var err error

	channel := q.MustChan()

	queue, err := channel.QueueDeclare(
		"",    // name of the queue
		false, // durable
		true,  // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Queue Declare: %s", err))
	}

	t := &Transfer{
		Checksum:  change.Checksum,
		Path:      change.Path,
		Requestor: hostname,
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
			ReplyTo:     queue.Name,
		})

	go WaitForFile(channel, change.Path, queue.Name)
}

func WaitForFile(channel *amqp.Channel, filePath string, queueName string) {
	defer channel.Close()

	<-LockFile(filePath)
	defer UnlockFile(filePath)

	msgs, err := channel.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		true,      // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		panic(fmt.Errorf("Failed to register a consumer: %s", err.Error()))
	}
	log.Println("waiting for file to be transfered")

	fullPath := filepath.Join(rootDir, filePath)
	file, err := os.OpenFile(fullPath, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0600)

	if err != nil {
		log.Fatalf("couldn't open local file %s", fullPath, err)
	}
	defer file.Close()

	for {
		msg := <-msgs
		order, err := strconv.Atoi(msg.Headers["Order"].(string))
		if err != nil {
			log.Println(fmt.Errorf("no order was present in the header for file transfer"))
			break
		}
		chunk := &FileChunk{
			order,
			msg.Body,
		}
		if chunk.Order == -1 {
			break
		}

		log.Printf("writing chunk %d for file %s\n", chunk.Order, file.Name())
		file.Write(chunk.Chunk)
	}

	log.Println("finished receiving file", filePath)
}

type FileChunk struct {
	Order int
	Chunk []byte
}

func handle(changes <-chan amqp.Delivery) {
	for msg := range changes {
		msg.Ack(false)

		var change Change
		if err := json.Unmarshal(msg.Body, &change); err != nil {
			log.Printf("Could not unmarshal", string(msg.Body))
			return
		}
		if change.Source == hostname {
			return
		}
		go HandleChange(&change)
	}
	log.Printf("CONSUMER: handle: changes channel closed")
}

func HandleChange(change *Change) {
	if change.IsCreate || change.IsMod {
		go RequestFile(change)
		return
	}

	fullPath := filepath.Join(rootDir, change.Path)
	local, infoErr := os.Stat(fullPath)

	// move means this file has been moved and we will receive another event for a new file
	// so we can treat it as a delete
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
