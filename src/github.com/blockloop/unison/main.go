package main

import "log"

var (
	hostname string
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	go WatchLocalChanges()
	go ListenForChanges()
	go ListenForTransferRequests()

	// block
	select {}
}
