package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"strconv"
)

const (
	ColumnIDSize = 64
	ColumnUserNameSize = 32
	ColumnEmailSize = 255
	ColumnEmailOffset = ColumnIDSize + ColumnUserNameSize
	RowSize = ColumnIDSize + ColumnEmailSize + ColumnUserNameSize
)

const CutSet = string(rune(0))

type Row struct {
	ID uint64
	Username string
	Email string
}

func NewRow(idStr, username, email string) (*Row, error) {
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return nil, errors.Wrapf(PrepareSyntaxError, "parse insert statement arg %s error", idStr)
	}

	if len([]byte(username)) > ColumnUserNameSize {
		return nil, errors.Wrapf(PrepareStringTooLongError, "username = %s", username)
	}

	if len([]byte(email)) > ColumnEmailSize {
		return nil, errors.Wrapf(PrepareStringTooLongError, "email = %s", email)
	}

	return &Row{
		ID:       id,
		Username: username,
		Email:    email,
	}, nil
}

func (r *Row) String() string {
	return fmt.Sprintf("Row(ID=%d, Username=%s, Email=%s)", r.ID, r.Username, r.Email)
}

func (r *Row) serialize() []byte {
	buffer := make([]byte, RowSize)
	binary.LittleEndian.PutUint64(buffer[:ColumnIDSize], r.ID)
	copy(buffer[ColumnIDSize:ColumnIDSize + ColumnUserNameSize], r.Username)
	copy(buffer[ColumnIDSize + ColumnUserNameSize:], r.Email)
	return buffer
}


func deserialize(buffer []byte) *Row {
	id := binary.LittleEndian.Uint64(buffer[:ColumnIDSize])
	username := string(bytes.Trim(buffer[ColumnIDSize: ColumnEmailOffset], CutSet))
	email := string(bytes.Trim(buffer[ColumnEmailOffset:], CutSet))
	return &Row{
		ID: id,
		Username: username,
		Email: email,
	}
}
