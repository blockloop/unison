// This example declares a durable Exchange, and publishes a single message to
// that Exchange with a given routing key.
//
package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/blockloop/unison/q"

	"github.com/howeyc/fsnotify"
	"github.com/streadway/amqp"
)

var (
	rootDir string
)

func init() {
	rootDir = filepath.Join(os.Getenv("HOME"), "Unison")
}

func WatchLocalChanges() {
	channel := q.MustChan()

	go declareExchange(channel)

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
					handleChange(channel, ev)
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

func declareExchange(channel *amqp.Channel) {
	log.Printf("PUBLISHER: got Channel, declaring %q Exchange (%q)", "fanout", "changes")
	if err := channel.ExchangeDeclare(
		"changes", // name
		"fanout",  // type
		true,      // durable
		true,      // auto-deleted
		false,     // internal
		false,     // noWait
		nil,       // arguments
	); err != nil {
		log.Fatalf("Exchange Declare: %s", err)
	}

	log.Printf("PUBLISHER: declared Exchange")
}

func handleChange(channel *amqp.Channel, ev *fsnotify.FileEvent) error {
	// check to see if the file is locked. If it is then break
	path, _ := filepath.Rel(rootDir, ev.Name)
	if FileIsLocked(path) {
		return nil
	}
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
		"changes", // publish to an exchange
		"",        // routing to 0 or more queues
		false,     // mandatory
		false,     // immediate
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
