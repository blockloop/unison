package main

import (
	"fmt"
	"log"
	"os"

	"github.com/streadway/amqp"
)

var (
	amqpURI     = "amqp://***REMOVED***@owl.rmq.cloudamqp.com/***REMOVED***" // "AMQP URI"
	bindingKey  = ""                                                                                // "AMQP binding key"
	consumerTag = ""                                                                                // "AMQP consumer tag (should not be blank)"
	exchange    = "headers"
	queueName   string
	hostname    string
)

func init() {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		panic(err)
	}
	queueName = fmt.Sprintf("%s", hostname)
}

func main() {
	c, err := NewConsumer(amqpURI)
	if err != nil {
		// log.Fatalf("%s", err)
		panic(err)
	}

	select {}

	if err := c.Shutdown(); err != nil {
		log.Fatalf("CONSUMER: error during shutdown: %s", err)
	}

}

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func NewConsumer(amqpURI string) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     "",
		done:    make(chan error),
	}

	var err error

	log.Printf("CONSUMER: dialing %s", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	log.Printf("CONSUMER: got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Println("CONSUMER: got Channel, declaring Exchange")
	if err = c.channel.ExchangeDeclare(
		exchange, // name of the exchange
		"fanout", // type
		true,     // durable
		true,     // delete when complete
		false,    // internal
		false,    // noWait
		nil,      // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Printf("CONSUMER: declared Exchange, declaring Queue (%s)", queueName)
	state, err := c.channel.QueueDeclare(
		queueName,    // name of the queue
		true,         // durable
		false,        // delete when usused
		false,        // exclusive
		false,        // noWait
		amqp.Table{}, // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Printf("CONSUMER: declared Queue (%d messages, %d consumers), binding to Exchange (key '%s')",
		state.Messages, state.Consumers, "")

	if err = c.channel.QueueBind(
		queueName, // name of the queue
		"",        // bindingKey
		"headers", // sourceExchange
		false,     // noWait
		nil,       // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	log.Printf("CONSUMER: Queue bound to Exchange, starting Consume (consumer tag '%s')", c.tag)
	deliveries, err := c.channel.Consume(
		queueName, // name
		c.tag,     // consumerTag,
		false,     // noAck
		false,     // exclusive
		false,     // noLocal
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	go handle(deliveries, c.done)

	return c, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("CONSUMER: AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	for {
		d := <-deliveries
		log.Printf(
			"CONSUMER: got %dB delivery: [%v] %s",
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)
		d.Ack(true)
	}
	log.Printf("CONSUMER: handle: deliveries channel closed")
}
