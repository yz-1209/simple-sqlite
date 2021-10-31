package main

import (
    "bufio"
    "fmt"
    "os"
    "strings"

    "github.com/pkg/errors"
    log "github.com/sirupsen/logrus"
    "github.com/yz-1209/simple_sqlite/db"
)

func init() {
    log.SetFormatter(&log.TextFormatter{})
    log.SetOutput(os.Stdout)
    log.SetLevel(log.InfoLevel)
}

func main() {
    if len(os.Args) < 2 {
        fmt.Println("Must supply a database filename.")
        os.Exit(1)
    }

    if len(os.Args) >= 3 && os.Args[2] == "warn" {
        log.SetLevel(log.WarnLevel)
    }

    users := db.NewTable(os.Args[1])
    if err := users.Open(); err != nil {
        panic(err)
    }

    Buffer := bufio.NewReader(os.Stdin)
    var command string
    var err error
    for {
        printPrompt()
        if command, err = Buffer.ReadString('\n'); err != nil {
            fmt.Println(errors.Wrapf(err, "read string error"))
            panic(err)
        }

        executeCommand, err := db.PrepareCommand(strings.TrimSpace(command), users)
        if err != nil {
            fmt.Printf("%+v\n", err)
            continue
        }

        if err = executeCommand.Execute(); err != nil {
            fmt.Printf("%+v\n", err)
            continue
        }

        fmt.Println("Executed.")
    }
}

func printPrompt() {
    fmt.Printf("db > ")
}
