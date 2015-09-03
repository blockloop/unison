// This example declares a durable Exchange, and publishes a single message to
// that Exchange with a given routing key.
//
package main

import (
	"encoding/json"
	"log"
	"path/filepath"
	"time"

	"github.com/howeyc/fsnotify"
	"github.com/streadway/amqp"
)

// func init() {
// 	// for {
// 	// 	reader := bufio.NewReader(os.Stdin)
// 	// 	fmt.Print("Enter text: ")
// 	// 	text, _ := reader.ReadString('\n')
// 	// 	if err := publish(amqpURI, text); err != nil {
// 	// 		log.Fatalf("%s", err)
// 	// 	}
// 	// 	log.Printf("published %dB OK", len([]byte(text)))
// 	// }
// }

const (
	rootDir = "/Users/brett/Unison"
)

var (
	channel    *amqp.Channel
	connection *amqp.Connection
)

func init() {
	go connectAmqp()
	go fsWatch()
}

func fsWatch() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}

	// Process events
	go func() {
		for {
			select {
			case ev := <-watcher.Event:
				// log.Println("event:", ev)
				if !ev.IsAttrib() {
					publish(ev)
				}
			case err := <-watcher.Error:
				log.Println("PUBLISHER: error:", err)
			}
		}
		watcher.Close()
	}()

	err = watcher.Watch(rootDir)
	if err != nil {
		log.Fatal(err)
	}
}

func connectAmqp() {
	log.Printf("PUBLISHER: dialing %q", amqpURI)
	var err error

	connection, err = amqp.Dial(amqpURI)
	if err != nil {
		log.Fatalf("Dial: %s", err)
	}

	log.Printf("PUBLISHER: got Connection, getting Channel")
	channel, err = connection.Channel()
	if err != nil {
		log.Fatalf("ERROR Channel: %s", err)
	}

	log.Printf("PUBLISHER: got Channel, declaring %q Exchange (%q)", "fanout", exchange)
	if err := channel.ExchangeDeclare(
		exchange, // name
		"fanout", // type
		true,     // durable
		true,     // auto-deleted
		false,    // internal
		false,    // noWait
		nil,      // arguments
	); err != nil {
		log.Fatalf("Exchange Declare: %s", err)
	}

	log.Printf("PUBLISHER: declared Exchange")
}

// This function dials, connects, declares, publishes, and tears down,
// all in one go. In a real service, you probably want to maintain a
// long-lived connection as state, and publish against that.
func publish(ev *fsnotify.FileEvent) error {
	path, _ := filepath.Rel(rootDir, ev.Name)
	log.Printf("PUBLISHER: publishing %dB path (%q)", len(path), path)

	var change = &Change{
		IsCreate: ev.IsCreate(),
		IsDelete: ev.IsDelete(),
		IsMod:    ev.IsModify(),
		IsMove:   ev.IsRename(),
		ModDate:  time.Now().UTC(),
		Path:     path,
		Source:   hostname,
		Checksum: Checksum(ev.Name),
	}

	msg, err := json.Marshal(change)
	if err != nil {
		log.Fatalf("ERROR marshaling msg %s", change)
	}

	if err := channel.Publish(
		exchange, // publish to an exchange
		"",       // routing to 0 or more queues
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            msg,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
			/*

				ContentType     string    // MIME content type
				ContentEncoding string    // MIME content encoding
				DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
				Priority        uint8     // 0 to 9
				CorrelationId   string    // correlation identifier
				ReplyTo         string    // address to to reply to (ex: RPC)
				Expiration      string    // message expiration spec
				MessageId       string    // message identifier
				Timestamp       time.Time // message timestamp
				Type            string    // message type name
				UserId          string    // creating user id - ex: "guest"
				AppId           string    // creating application id
			*/
		},
	); err != nil {
		log.Fatalf("Exchange Publish: %s", err)
		panic(err)
	}

	return nil
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("PUBLISHER: waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("PUBLISHER: confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("PUBLISHER: failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}
