package db

import (
	"errors"
)

var InsertFullTableError = errors.New("error: insert rows into full table")

var PrepareSyntaxError = errors.New("error: could not parse statement")

var PrepareUnrecognizedStatementError = errors.New("error: unrecognized statement")

var UnrecognizedMetaCommandError = errors.New("error: unrecognized meta command")

var PrepareStringTooLongError = errors.New("error: string is too long")
