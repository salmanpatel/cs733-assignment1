package main

import (
	"fmt"
	"net"
//	"regexp"
	"bufio"
	"strings"
//	"sync"
	"strconv"
	"io"
	"time"
	"math"
)

type File struct {
	fileName string
	fileSize uint64
	fileVersionNo uint64
	fileExpTime uint64
	fileContent []byte
}

var psuedoTime uint64 = 0
const initialVersionNo uint64 = 1
var fileSystem map[string]File

func writeFile(conn net.Conn ,cmdTokens []string) {
	fmt.Println("Inside write file")
	conn.Write([]byte(""))
	fileSize, err := strconv.ParseUint(cmdTokens[2],10,64)
	if err != nil {
		conn.Write([]byte("ERR_CMD_ERR\r\n"))
		conn.Close()
		return
	}
	var contentBuffer []byte = make([]byte,fileSize)
	n, err := io.ReadFull(conn, contentBuffer)
//	fmt.Println(contentBuffer)
//	fmt.Println(n)
	if err != nil || uint64(n) < fileSize {
		conn.Write([]byte("ERR_INTERNAL\r\n"))
		conn.Close()
		return
	}
	// Reading and discarding "\r\n"
	bufio.NewReader(conn).ReadByte()
//	bufio.NewReader(conn).ReadByte()
//	contentBuffer = nil
	var expTimeUser uint64
	var expTime uint64 = math.MaxUint64
	if(len(cmdTokens)==4) {
		expTimeUser, err = strconv.ParseUint(cmdTokens[3],10,64)
		if err != nil {
			conn.Write([]byte("ERR_CMD_ERR\r\n"))
			return
		}
		expTime = psuedoTime + expTimeUser
	}
	var versionNo uint64 = initialVersionNo
	if fileObj, ok := fileSystem[cmdTokens[1]]; ok  {
//	if _, ok := fileSystem[cmdTokens[1]]; ok  {
		// present in dict
		fileObj.fileContent = contentBuffer
		fileObj.fileVersionNo += 1
		fileObj.fileSize = fileSize
		fileObj.fileExpTime = expTime
		fileSystem[cmdTokens[1]] = fileObj
		versionNo = fileObj.fileVersionNo
	} else {
		fileSystem[cmdTokens[1]] = File{cmdTokens[1],fileSize, initialVersionNo, expTime, contentBuffer}
	}
//	conn.Write([]byte("OK " + fileObj.fileVersionNo + "\r\n"))
	conn.Write([]byte("OK " + strconv.FormatUint(versionNo,10) + "\r\n"))
}

func readFile(conn net.Conn, cmdTokens []string) {
	fmt.Println("Inside read file")
	if fileObj, ok := fileSystem[cmdTokens[1]]; !ok || fileObj.fileExpTime < psuedoTime {
		conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
		return
        } else {
		expTime := ""
		if fileObj.fileExpTime != math.MaxUint64 {
			fmt.Println("psuedoTime" + strconv.FormatUint(psuedoTime,10))
			fmt.Println("fileObj.fileExpTime" + strconv.FormatUint(fileObj.fileExpTime,10))
			expTime = " " + strconv.FormatUint(fileObj.fileExpTime-psuedoTime,10)
		}
		returnVal := "CONTENTS " + strconv.FormatUint(fileObj.fileVersionNo, 10) + " " + strconv.FormatUint(fileObj.fileSize, 10) + expTime + "\r\n"
		returnVal2 := append([]byte(returnVal), fileObj.fileContent...)
		conn.Write(append(returnVal2, []byte("\r\n")...))
	}
}

func casFile(conn net.Conn, cmdTokens []string) {
	fmt.Println("Inside CAS file")
	conn.Write([]byte(""))
	fileSize, err1 := strconv.ParseUint(cmdTokens[3],10,64)
	if err1 != nil  {
		conn.Write([]byte("ERR_CMD_ERR\r\n"))
		conn.Close()
		return
	}
	var contentBuffer []byte = make([]byte,fileSize)
	n, err := io.ReadFull(conn, contentBuffer)
	fmt.Println(contentBuffer)
	fmt.Println(n)
	if err != nil || uint64(n) < fileSize {
		conn.Write([]byte("ERR_INTERNAL\r\n"))
		conn.Close()
		return
	}
	// Reading and discarding "\r\n"
	bufio.NewReader(conn).ReadByte()
	versionNo, err2 := strconv.ParseUint(cmdTokens[2],10,64)
	if err2 != nil {
		conn.Write([]byte("ERR_CMD_ERR\r\n"))
		return
	}
	var expTimeUser uint64
	var err3 error
	var expTime uint64 = math.MaxUint64
	if(len(cmdTokens)==5) {
		expTimeUser, err3 = strconv.ParseUint(cmdTokens[4],10,64)
		if err3 != nil {
			conn.Write([]byte("ERR_CMD_ERR\r\n"))
			return
		}
		expTime = psuedoTime + expTimeUser
	}
	var fileObj File
	var ok bool
	if fileObj, ok = fileSystem[cmdTokens[1]]; !ok  {
		conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
		return
        } else {
		if versionNo != fileObj.fileVersionNo {
			conn.Write([]byte("ERR_VERSION\r\n"))
			return
		}
		fileObj.fileVersionNo += 1
		fileObj.fileContent = contentBuffer
		fileObj.fileSize = fileSize
		fileObj.fileExpTime = expTime
		fileSystem[cmdTokens[1]] = fileObj
	        conn.Write([]byte("OK " + strconv.FormatUint(fileSystem[cmdTokens[1]].fileVersionNo,10) + "\r\n"))
	}
}

func deleteFile(conn net.Conn, cmdTokens []string) {
	fmt.Println("Inside delete file")
	if _, ok := fileSystem[cmdTokens[1]]; !ok  {
		conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
		return
        } else {
		delete(fileSystem, cmdTokens[1])
		conn.Write([]byte("OK\r\n"))
	}
}

func processCommand(conn net.Conn, command string) {
	tokens := strings.Fields(command)
	if(len(tokens) == 0) {
		return
	}
	fmt.Printf("tokens %v\n", tokens)
	fmt.Printf("length of tokens %v\n", len(tokens))
	switch tokens[0] {
		 case "write":
			if len(tokens)==3 || len(tokens)==4 {
				writeFile(conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 case "read":
			if len(tokens)==2 {
				readFile(conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 case "cas":
			if len(tokens)==4 || len(tokens)==5 {
				casFile(conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 case "delete":
			if len(tokens)==2 {
				deleteFile(conn, tokens)
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

func incTimer() {
	ticker := time.NewTicker(time.Second * 1)
	go func() {
		for _ = range ticker.C {
			psuedoTime += 1
		}
	}()
}

func main() {
	go incTimer()
	fileSystem = make(map[string]File)
	serverMain()
}
