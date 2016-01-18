package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"math"
	"os"
)

// 8KB
const filechunk = 8192

func Checksum(path string) string {
	file, err := os.Open(path)

	if err != nil {
		log.Printf("couldn't checksum file %s", path)
		return ""
	}

	defer file.Close()

	// calculate the file size
	info, _ := file.Stat()

	filesize := info.Size()

	blocks := uint64(math.Ceil(float64(filesize) / float64(filechunk)))

	hash := md5.New()

	for i := uint64(0); i < blocks; i++ {
		blocksize := int(math.Min(filechunk, float64(filesize-int64(i*filechunk))))
		buf := make([]byte, blocksize)

		file.Read(buf)
		io.WriteString(hash, string(buf)) // append into the hash
	}

	return fmt.Sprintf("%x", hash.Sum(nil))
}
