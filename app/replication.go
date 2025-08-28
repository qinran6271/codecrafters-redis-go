package main

import (
    "crypto/rand"
    "fmt"
    // "strconv"
    // "strings"
)

const replidChars = "abcdefghijklmnopqrstuvwxyz0123456789"

var masterReplId = generateReplId()
var masterReplOffset = 0
var masterHost string
var masterPort int

func generateReplId() string {
    b := make([]byte, 40)
    _, err := rand.Read(b)
    if err != nil {
        panic(err)
    }
    for i := 0; i < 40; i++ {
        b[i] = replidChars[int(b[i])%len(replidChars)]
    }
    return string(b)
}

// func parseReplicaof(replicaof string) {
//     parts := strings.Split(replicaof, " ")
//     masterHost = parts[0]
//     masterPort, _ = strconv.Atoi(parts[1])
// }

func GetReplicationInfo() string {
    if role == "master" {
        return fmt.Sprintf(
            "# Replication\r\nrole:master\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\n",
            masterReplId, masterReplOffset,
        )
    }
    return fmt.Sprintf(
        "# Replication\r\nrole:slave\r\nmaster_host:%s\r\nmaster_port:%d\r\n",
        masterHost, masterPort,
    )
}

