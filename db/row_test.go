package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowSerialize(t *testing.T) {
	row := &Row{
		ID: 1,
		Username: "tony",
		Email: "sqlite@google.com",
	}

	b := row.serialize()
	assert.Equal(t, len(b), RowSize)

	dRow := deserialize(b)
	assert.Equal(t, dRow.ID, row.ID)
	assert.Equal(t, dRow.Username, row.Username)
	assert.Equal(t, dRow.Email, row.Email)
}
