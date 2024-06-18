package main

import (
    "encoding/json"
    "fmt"
    "net"
    "os"
    "strings"
)

type RootConfig struct {
    Address  string   `json:"address"`
    Replicas []string `json:"replicas"`
}

type QueryResponse struct {
    Filename    string `json:"filename"`
    Occurrences int    `json:"occurrences"`
}

func main() {
    configFile, err := os.ReadFile("root_config.json")
    if err != nil {
        panic(err)
    }

    var config RootConfig
    err = json.Unmarshal(configFile, &config)
    if err != nil {
        panic(err)
    }

    ln, err := net.Listen("tcp", config.Address)
    if err != nil {
        panic(err)
    }
    defer ln.Close()
    fmt.Println("Root node listening on", config.Address)

    for {
        conn, err := ln.Accept()
        if err != nil {
            panic(err)
        }
        go handleClient(conn, config.Replicas)
    }
}

func handleClient(conn net.Conn, replicas []string) {
    defer conn.Close()

    buf := make([]byte, 1024)
    n, err := conn.Read(buf)
    if err != nil {
        panic(err)
    }

    query := string(buf[:n])
    keywords := strings.Fields(query)
    numReplicas := len(replicas)
    chunkSize := (len(keywords) + numReplicas - 1) / numReplicas

    responses := make(chan []QueryResponse, numReplicas)
    for i, replica := range replicas {
        start := i * chunkSize
        end := start + chunkSize
        if end > len(keywords) {
            end = len(keywords)
        }

        go func(replica string, keywords []string) {
            conn, err := net.Dial("tcp", replica)
            if err != nil {
                panic(err)
            }
            defer conn.Close()

            query := strings.Join(keywords, " ")
            conn.Write([]byte(query))

            buf := make([]byte, 4096)
            n, err := conn.Read(buf)
            if err != nil {
                panic(err)
            }

            var response []QueryResponse
            err = json.Unmarshal(buf[:n], &response)
            if err != nil {
                panic(err)
            }

            responses <- response
        }(replica, keywords[start:end])
    }

    var finalResults []QueryResponse
    for i := 0; i < numReplicas; i++ {
        response := <-responses
        finalResults = append(finalResults, response...)
    }

    responseBytes, err := json.Marshal(finalResults)
    if err != nil {
        panic(err)
    }
    conn.Write(responseBytes)
}