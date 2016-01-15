package main

import (
	"fmt"
	"net"
//	"regexp"
	"bufio"
	"strings"
	"sync"
)

type File struct {
	fileLock sync.RWMutex
	fileName string
	fileSize uint64
	fileVersionNo uint64
	fileExpTime uint64
	fileContent* []byte
}

var fileSystem map[string]File

func writeFile(conn net.Conn ,cmdTokens []string) {
	fmt.Println("Inside write file")
//	var buffer string 
//s	buffer,_ := bufio.NewReader(conn).ReadString('\n')
//s	fmt.Print(buffer)
//	fmt.Println(err)
//s	if _, ok := fileSystem[cmdTokens[1]]; ok  {
		// present in dict
//s		fmt.Println(buffer)
//s	}
	conn.Write([]byte("OK <<Version>>\r\n"))
}

func readFile(conn net.Conn, cmdTokens []string) {
	fmt.Println("Inside read file")
	conn.Write([]byte("<<File Content>>\r\n"))
}

func casFile(conn net.Conn, cmdTokens []string) {
	fmt.Println("Inside CAS file")
	conn.Write([]byte("OK <<Version>>\r\n"))
}

func deleteFile(conn net.Conn, cmdTokens []string) {
	fmt.Println("Inside delete file")
	conn.Write([]byte("OK\r\n"))
}

func processCommand(conn net.Conn, command string) {
	tokens := strings.Fields(command)
	fmt.Printf("tokens %v\n", tokens)
	fmt.Printf("length of tokens %v\n", len(tokens))
	switch tokens[0] {
		 case "write":
			if len(tokens)==3 || len(tokens)==4 {
				go writeFile(conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 case "read":
			if len(tokens)==2 {
				go readFile(conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 case "cas":
			if len(tokens)==4 || len(tokens)==5 {
				go casFile(conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 case "delete":
			if len(tokens)==2 {
				go deleteFile(conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 default:
			fmt.Println("Bad Command")
			conn.Write([]byte("ERR_CMD_ERR\r\n"))
	}
}

func handleClient(c net.Conn) {
	fmt.Printf("Client %v connected.\n", c.RemoteAddr())
	for {
		fmt.Println("Waiting for command")
		buffer,_ := bufio.NewReader(c).ReadString('\n')
		fmt.Printf("Message Read : %s", buffer)
		processCommand(c, buffer)
		fmt.Println("Command processed")
	}
//	fmt.Printf("Client connection closed from %v.\n", c.RemoteAddr())
}

func serverMain() {
	listenPort := "8080"
	listenConn, err := net.Listen("tcp", ":"+listenPort)
	if err != nil {
		fmt.Println("Error opening Listen Socket on %s port", listenPort)
	}
	fmt.Printf("Server up and listening on port %s\n", listenPort)
	for {
		conn, err := listenConn.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleClient(conn)
	}
}

func main() {
	fileSystem = make(map[string]File)
	serverMain()
}
