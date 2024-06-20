package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run replica.go <replica_address>")
		return
	}

	replicaAddress := os.Args[1]
	listener, err := net.Listen("tcp", replicaAddress)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Replica listening on", replicaAddress)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	message, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading message:", err)
		return
	}
	message = strings.TrimSpace(message)
	keywords := strings.Split(message, ",")
	fmt.Printf("Received query: %v\n", keywords)

	// Read files and count occurrences
	fileOccurrences := make(map[string]int)
	files, err := os.ReadDir(".")
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	for _, file := range files {
		if !file.IsDir() {
			fileName := file.Name()
			occurrences := countOccurrences(fileName, keywords)
			fileOccurrences[fileName] = occurrences
		}
	}

	response := ""
	for fileName, count := range fileOccurrences {
		response += fmt.Sprintf("(%s,%d) ", fileName, count)
	}
	fmt.Printf("Sending response: %s\n", response)

	conn.Write([]byte(response + "\n"))
}

func countOccurrences(fileName string, keywords []string) int {
	file, err := os.Open(fileName)
	if err != nil {
		return 0
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		for _, keyword := range keywords {
			count += strings.Count(line, keyword)
		}
	}

	return count
}
