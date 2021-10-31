package db

import (
	"fmt"
	"os"
	"strings"
)

type Command interface {
	Execute() error
}

type ExitCommand struct{
	table *Table
}

func (ec *ExitCommand) Execute() (err error) {
	if err = ec.table.Close(); err != nil {
		panic(err)
	}

	fmt.Println("bye!")
	os.Exit(0)
	return
}

func PrepareMetaCommand(command string, table *Table) (Command, error) {
	if command == ".exit" {
		return &ExitCommand{table: table}, nil
	}

	return nil, UnrecognizedMetaCommandError
}

func PrepareCommand(command string, table *Table)  (Command, error) {
	if strings.HasPrefix(command, ".") {
		return PrepareMetaCommand(command, table)
	}

	return PrepareStatement(command, table)
}
