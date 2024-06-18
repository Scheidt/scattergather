package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net"
    "os"
    "strings"
)

type QueryResponse struct {
    Filename    string `json:"filename"`
    Occurrences int    `json:"occurrences"`
}

func main() {
    configFile, err := os.ReadFile("replicas_config.json")
    if err != nil {
        panic(err)
    }

    var replicas []map[string]string
    err = json.Unmarshal(configFile, &replicas)
    if err != nil {
        panic(err)
    }

    for _, replica := range replicas {
        address := replica["address"]
        go startReplica(address)
    }

    select {} // keep the main function running
}

func startReplica(address string) {
    ln, err := net.Listen("tcp", address)
    if err != nil {
        panic(err)
    }
    defer ln.Close()
    fmt.Println("Replica listening on", address)

    for {
        conn, err := ln.Accept()
        if err != nil {
            panic(err)
        }
        go handleRootRequest(conn)
    }
}

func handleRootRequest(conn net.Conn) {
    defer conn.Close()

    buf := make([]byte, 1024)
    n, err := conn.Read(buf)
    if err != nil {
        panic(err)
    }

    keywords := strings.Fields(string(buf[:n]))
    var results []QueryResponse

    files, err := ioutil.ReadDir("./texts")
    if err != nil {
        panic(err)
    }

    for _, file := range files {
        content, err := ioutil.ReadFile("./texts/" + file.Name())
        if err != nil {
            panic(err)
        }

        contentStr := string(content)
        occurrences := 0
        for _, keyword := range keywords {
            occurrences += strings.Count(contentStr, keyword)
        }

        if occurrences > 0 {
            results = append(results, QueryResponse{Filename: file.Name(), Occurrences: occurrences})
        }
    }

    responseBytes, err := json.Marshal(results)
    if err != nil {
        panic(err)
    }
    conn.Write(responseBytes)
}