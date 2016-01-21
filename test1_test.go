package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
//	"strings"
	"testing"
	"time"
	"runtime"
)

var c = make(chan bool)

func TestConcurrency(t *testing.T) {
	runtime.GOMAXPROCS(20)
	go serverMain()
        time.Sleep(1 * time.Second) // one second is enough time for the server to start
	noOfClients := 10
	for i:=0 ; i<noOfClients; i++ {
		go sendMessage(t, i)
	}
	for _ = range c {
	}
}

// Simple serial check of getting and setting
func sendMessage(t *testing.T, clientNo int) {
	name := "hi.txt"
//	contents := ""
	exptime := 300000
	conn, err := net.DialTimeout("tcp", "localhost:8080", 1 * time.Second)
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	scanner := bufio.NewScanner(conn)

	totWriteOp := 10
	// Write a file
	for j:=0; j<totWriteOp; j++ {
		contents := strconv.FormatInt(int64(10*clientNo+j),10)
		fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
		scanner.Scan() // read first line
		_ = scanner.Text() // extract the text from the buffer
	}
}

// Useful testing function
/*
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}
*/
