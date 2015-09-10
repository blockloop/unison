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

	"bitbucket.org/justbrettjones/unison/q"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func ListenForTransferRequests() {
	channel := q.MustChan()

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
		false, // durable
		true,  // delete when usused
		true,  // exclusive
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
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		panic(fmt.Errorf("Failed to register a consumer %s", err.Error()))
	}

	log.Println("awaiting file transfer requests...")
	for msg := range msgs {
		msg.Ack(false)
		var transfer Transfer

		if err := json.Unmarshal(msg.Body, &transfer); err != nil {
			log.Println(fmt.Errorf("received a dirty transfer request '%s' %s", string(msg.Body), err.Error()))
			msg.Reject(false)
			continue
		}

		if transfer.Requestor == hostname {
			continue
		}
		log.Println("received a file transfer request", transfer.Path)

		go HandleTransfer(transfer, &msg)
	}
	log.Println("no longer consuming transfers")
}

func HandleTransfer(t Transfer, msg *amqp.Delivery) {
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
		panic(fmt.Errorf("could not open file %s %s", file, err.Error()))
		return
	}
	defer file.Close()

	count := int(math.Ceil(float64(info.Size()) / float64(1024)))

	ch := make(chan []byte, count)

	// read the file into a buffered channel
	go readFile(file, ch)

	channel := q.MustChan()
	defer channel.Close()

	// publish each chunk
	i := 1
	for chunk := range ch {
		log.Println("sending chunk", i)
		err = channel.Publish(
			"",          // exchange
			msg.ReplyTo, // routing key
			false,       // mandatory
			false,       // immediate
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

	log.Println("file transfer complete. sending EOF", t.Path)
	// send EOF
	err = channel.Publish(
		"",          // exchange
		msg.ReplyTo, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "application/octet-stream",
			Body:        nil,
			Headers: amqp.Table{
				"Order": strconv.Itoa(-1),
				"Count": strconv.Itoa(count),
			},
		})
}

func readFile(file *os.File, ch chan []byte) {
	off := 0
	for {
		buffer := make([]byte, 1024)
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}
		ch <- buffer[:n]
		off = off + n
	}
	close(ch)
}
