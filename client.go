package main

import (
	"fmt"
	"net"
	"bufio"
	"os"
)

var done chan bool

func readAndSend(conn net.Conn) {
	fmt.Println("inside read and send")
	for {
//		fmt.Printf("Enter Command : ")
		reader := bufio.NewReader(os.Stdin)
		msg,_ := reader.ReadString('\n')
		conn.Write([]byte(msg))
	}
	done<-true
}

func receiveAndDisplay(conn net.Conn) {
	for {
		buffer,_ := bufio.NewReader(conn).ReadString('\n')
		fmt.Printf("Message from Server : %s\n", buffer)
	}
	done<-true
}

func main() {
	host := "localhost"
	port := "8080"
	conn, err := net.Dial("tcp",host+":"+port)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Connection established between %s and localhost.\n", host)
	fmt.Printf("Remote Address : %s \n", conn.RemoteAddr().String())
	fmt.Printf("Local Address : %s \n", conn.LocalAddr().String())
	fmt.Println("Start entering commands ...")
/*	for {
		fmt.Printf("Enter Command : ")
		reader := bufio.NewReader(os.Stdin)
		msg,_ := reader.ReadString('\n')
		conn.Write([]byte(msg))
		
		buffer,_ := bufio.NewReader(conn).ReadString('\n')
		fmt.Printf("Message from Server : %s\n", buffer)
	}*/
	done = make(chan bool)
	go readAndSend(conn)
	go receiveAndDisplay(conn)
	<-done
}
