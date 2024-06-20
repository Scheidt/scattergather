package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

type RootConfig struct {
	RootAddress string   `json:"root_address"`
	Replicas    []string `json:"replicas"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run root.go <root_config_file>")
		return
	}

	rootConfigFile := os.Args[1]
	config := readConfig(rootConfigFile)
	rootAddress := config.RootAddress
	replicaAddresses := config.Replicas

	listener, err := net.Listen("tcp", rootAddress)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Root node listening on", rootAddress)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleClientRequest(conn, replicaAddresses)
	}
}

func readConfig(filename string) RootConfig {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening config file:", err)
		os.Exit(1)
	}
	defer file.Close()

	var config RootConfig
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		os.Exit(1)
	}

	return config
}

func handleClientRequest(conn net.Conn, replicaAddresses []string) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	query, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading query:", err)
		return
	}
	query = strings.TrimSpace(query)
	keywords := strings.Split(query, " ")
	fmt.Printf("Received query from client: %v\n", keywords)

	numReplicas := len(replicaAddresses)
	chunks := splitKeywords(keywords, numReplicas)

	resultsChan := make(chan string, numReplicas)
	for i, chunk := range chunks {
		go queryReplica(replicaAddresses[i], chunk, resultsChan)
	}

	results := ""
	for i := 0; i < numReplicas; i++ {
		results += <-resultsChan + "\n"
	}

	fmt.Printf("Sending combined results to client: %s\n", results)
	conn.Write([]byte(results))
}

func splitKeywords(keywords []string, numChunks int) [][]string {
	var chunks [][]string
	chunkSize := (len(keywords) + numChunks - 1) / numChunks
	for i := 0; i < len(keywords); i += chunkSize {
		end := i + chunkSize
		if end > len(keywords) {
			end = len(keywords)
		}
		chunks = append(chunks, keywords[i:end])
	}
	return chunks
}

func queryReplica(replicaAddress string, keywords []string, resultsChan chan string) {
	conn, err := net.Dial("tcp", replicaAddress)
	if err != nil {
		fmt.Println("Error connecting to replica:", err)
		resultsChan <- ""
		return
	}
	defer conn.Close()

	message := strings.Join(keywords, ",")
	fmt.Printf("Sending query to replica %s: %s\n", replicaAddress, message)
	conn.Write([]byte(message + "\n"))

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading response from replica:", err)
		resultsChan <- ""
		return
	}
	response = strings.TrimSpace(response)
	fmt.Printf("Received response from replica %s: %s\n", replicaAddress, response)

	resultsChan <- response
}
