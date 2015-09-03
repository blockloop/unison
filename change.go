package main

import "time"

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
