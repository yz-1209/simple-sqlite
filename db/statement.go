package db

import (
	"fmt"
	"github.com/pkg/errors"
	"strings"
)

type StatementType int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Statement struct {
	Type StatementType
	Row *Row
	Table *Table
}

func (s *Statement) Execute()  error {
	switch s.Type {
	case StatementInsert:
		if s.Table.IsFull() {
			return InsertFullTableError
		}

		return s.Table.AppendRow(s.Row)
	case StatementSelect:
		rows, err := s.Table.SelectRows()
		if err != nil {
			return err
		}

		for i := range rows {
			fmt.Println(rows[i])
		}
	}

	return nil
}

func PrepareStatement(command string, table *Table) (*Statement, error) {
	statement := &Statement{Table: table}
	if strings.HasPrefix(command, "insert") {
		statement.Type = StatementInsert
		args := strings.Split(strings.TrimSpace(strings.TrimPrefix(command, "insert")), " ")
		if len(args) < 3 {
			return nil, errors.Wrapf(PrepareSyntaxError, "insert statement args < 3")
		}

		row, err := NewRow(args[0], args[1], args[2])
		if err != nil {
			return nil, err
		}
		statement.Row = row
		return statement, nil
	}

	if strings.HasPrefix(command, "select") {
		statement.Type = StatementSelect
		return statement, nil
	}

	return statement, errors.Wrapf(PrepareUnrecognizedStatementError, "command = %s", command)
}
