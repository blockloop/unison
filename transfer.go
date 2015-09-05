package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func ListenForTransferRequests(channel *amqp.Channel) {
	if err := channel.ExchangeDeclare(
		"file-requests", // name of the exchange
		"fanout",        // type
		true,            // durable
		false,           // delete when complete
		false,           // internal
		false,           // noWait
		nil,             // arguments
	); err != nil {
		panic(fmt.Errorf("Exchange Declare: %s", err))
	}

	queue, err := channel.QueueDeclare(
		fmt.Sprintf("%s-files", hostname), // name of the queue
		true,  // durable
		true,  // delete when usused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Failed to declare a queue %s", err.Error()))
	}

	if err = channel.QueueBind(
		queue.Name,      // name of the queue
		"",              // bindingKey
		"file-requests", // sourceExchange
		false,           // noWait
		nil,             // arguments
	); err != nil {
		panic(fmt.Errorf("Queue Bind: %s", err))
	}

	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		panic(fmt.Errorf("Failed to set QoS %s", err.Error()))
	}

	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		panic(fmt.Errorf("Failed to register a consumer %s", err.Error()))
	}

	go func() {
		log.Println("awaiting file transfer requests...")
		for d := range msgs {
			var transfer Transfer
			log.Println("received a file transfer request", d.RoutingKey)

			if err := json.Unmarshal(d.Body, &transfer); err != nil {
				log.Println(fmt.Errorf("error could not unmarshal body of transfer request '%s' %s", string(d.Body), err.Error()))
				d.Reject(false)
				continue
			}
			log.Println("received transfer request for file:", transfer.Path)

			go d.Ack(false)
			go HandleTransfer(transfer)
		}
		log.Println("no longer consuming transfers")
	}()
}

func HandleTransfer(t Transfer) {
	fullPath := filepath.Join(rootDir, t.Path)
	info, err := os.Stat(fullPath)

	if err != nil {
		log.Println("we don't seem to have this file")
		return
	}

	hash := Checksum(fullPath)

	if hash != t.Checksum {
		log.Printf("our file hash does not match the requested hash. Ours %s. Theirs %s", hash, t.Checksum)
		return
	}

	log.Println("begin reading local file")

	var file *os.File
	if file, err = os.Open(fullPath); err != nil {
		log.Fatalln("could not open file", file, err.Error())
		return
	}
	defer file.Close()

	buffer := make([]byte, 1024)
	count := int(math.Ceil(float64(info.Size()) / float64(1024)))

	ch := make(chan []byte, count)

	go func() {
		for {
			n, err := file.Read(buffer)
			if err != nil && err != io.EOF {
				panic(err)
			}
			if n == 0 {
				break
			}
			ch <- buffer[:n]
		}
	}()

	channel := mustGetChan(amqpCon)
	defer channel.Close()

	i := 1
	for chunk := range ch {
		err = channel.Publish(
			"files", // exchange
			t.Path,  // routing key
			false,   // mandatory
			false,   // immediate
			amqp.Publishing{
				ContentType: "application/octet-stream",
				Body:        chunk,
				Headers: amqp.Table{
					"Order": strconv.Itoa(i),
					"Count": strconv.Itoa(count),
				},
			})

		if err != nil {
			panic(fmt.Errorf("Failed to publish a message %s", err.Error()))
		}
		i += 1
	}
}