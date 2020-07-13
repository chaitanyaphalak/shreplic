package dlog

import (
	"log"
	"os"
)

var DLOG = false

func Printf(format string, v ...interface{}) {
	if DLOG {
		log.Printf(format, v...)
	}
}

func Println(v ...interface{}) {
	if DLOG {
		log.Println(v...)
	}
}

func NewFileLogger(file *os.File) *log.Logger {
	return log.New(file, "", log.LstdFlags)
}
