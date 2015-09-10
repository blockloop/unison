package main

import (
	"log"
	"sync"
)

var (
	m     = &sync.Mutex{}
	locks = make(map[string]chan bool)
)

func LockFile(path string) chan bool {
	m.Lock()
	defer m.Unlock()

	if ch, locked := locks[path]; locked == true {
		log.Println("file is already locked", path)
		return ch
	} else {
		ch := make(chan bool, 1)
		locks[path] = ch

		ready := make(chan bool, 1)
		close(ready)
		return ready
	}
}

func FileIsLocked(path string) bool {
	m.Lock()
	defer m.Unlock()

	_, locked := locks[path]
	return locked
}

func UnlockFile(path string) {
	m.Lock()
	defer m.Unlock()
	if ch, locked := locks[path]; locked == true {
		close(ch)
		delete(locks, path)
	}
}
