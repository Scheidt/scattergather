package main

import (
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

type QueryResponse struct {
    Filename    string `json:"filename"`
    Occurrences int    `json:"occurrences"`
}

func main() {
    configFile, err := os.ReadFile("clients_config.json")
    if err != nil {
        panic(err)
    }

    var config ClientConfig
    err = json.Unmarshal(configFile, &config)
    if err != nil {
        panic(err)
    }

    for _, query := range config.Queries {
        sendQuery(config.RootAddress, query)
        time.Sleep(time.Duration(1+rand.Intn(2)) * time.Second)
    }
}

func sendQuery(address, query string) {
    conn, err := net.Dial("tcp", address)
    if err != nil {
        panic(err)
    }
    defer conn.Close()

    fmt.Println("Sending query:", query)
    conn.Write([]byte(query))

    buf := make([]byte, 4096)
    n, err := conn.Read(buf)
    if err != nil {
        panic(err)
    }

    var responses []QueryResponse
    err = json.Unmarshal(buf[:n], &responses)
    if err != nil {
        panic(err)
    }

    fmt.Println("Received responses:")
    for _, response := range responses {
        fmt.Printf("File: %s, Occurrences: %d\n", response.Filename, response.Occurrences)
    }
}
