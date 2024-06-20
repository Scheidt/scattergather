package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"
)

type ClientConfig struct {
	RootAddress string   `json:"root_address"`
	Queries     []string `json:"queries"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <client_config_file>")
		return
	}

	clientConfigFile := os.Args[1]
	config := readConfig(clientConfigFile)
	rootAddress := config.RootAddress
	queries := config.Queries

	for _, query := range queries {
		time.Sleep(time.Duration(rand.Intn(2)+1) * time.Second)
		sendQuery(rootAddress, query)
	}
}

func readConfig(filename string) ClientConfig {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening config file:", err)
		os.Exit(1)
	}
	defer file.Close()

	var config ClientConfig
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		os.Exit(1)
	}

	return config
}

func sendQuery(rootAddress, query string) {
	conn, err := net.Dial("tcp", rootAddress)
	if err != nil {
		fmt.Println("Error connecting to root node:", err)
		return
	}
	defer conn.Close()

	fmt.Printf("Sending query: %s\n", query)
	conn.Write([]byte(query + "\n"))

	reader := bufio.NewReader(conn)
	fmt.Println("Created Reader")
	response, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading response from root node:", err)
		return
	}
	fmt.Printf("Received response: %s\n", response)
}
